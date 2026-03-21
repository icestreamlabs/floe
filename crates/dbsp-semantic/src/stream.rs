use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::values::{GroupValue, ZeroValue};

type NodeId = usize;
type InputId = usize;
type BindingTrace = Vec<NodeId>;
type CacheKey = (NodeId, BindingTrace, usize);

static NEXT_NODE_ID: AtomicUsize = AtomicUsize::new(1);
static NEXT_INPUT_ID: AtomicUsize = AtomicUsize::new(1);

fn next_node_id() -> NodeId {
    NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed)
}

fn next_input_id() -> InputId {
    NEXT_INPUT_ID.fetch_add(1, Ordering::Relaxed)
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StreamNodeKind {
    Constant,
    Prefix,
    Source { name: Arc<str> },
    Input { name: Arc<str> },
    Lift { name: Arc<str> },
    Zip { name: Arc<str> },
    Delay,
    Feedback { name: Arc<str> },
    BindInput { input: Arc<str> },
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamPlanNode {
    pub(crate) id: usize,
    pub(crate) kind: StreamNodeKind,
    pub(crate) children: Vec<usize>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamPlan {
    pub(crate) root: usize,
    pub(crate) nodes: Vec<StreamPlanNode>,
}

impl StreamPlan {
    fn from_root(root: Arc<PlanNodeRef>) -> Self {
        let mut visited = HashSet::new();
        let mut nodes = Vec::new();
        collect_plan(root.clone(), &mut visited, &mut nodes);
        nodes.sort_by_key(|node| node.id);
        Self {
            root: root.id,
            nodes,
        }
    }

    pub(crate) fn normalized(&self) -> Self {
        let nodes_by_id: HashMap<_, _> = self.nodes.iter().map(|node| (node.id, node)).collect();
        let mut old_to_new = HashMap::new();
        let mut nodes = Vec::new();
        let root = normalize_plan(self.root, &nodes_by_id, &mut old_to_new, &mut nodes);
        Self { root, nodes }
    }

    #[allow(dead_code)]
    pub(crate) fn contains_kind(&self, predicate: impl Fn(&StreamNodeKind) -> bool) -> bool {
        self.nodes.iter().any(|node| predicate(&node.kind))
    }
}

#[allow(dead_code)]
fn collect_plan(
    node: Arc<PlanNodeRef>,
    visited: &mut HashSet<NodeId>,
    out: &mut Vec<StreamPlanNode>,
) {
    if !visited.insert(node.id) {
        return;
    }

    let children = node.children();
    out.push(StreamPlanNode {
        id: node.id,
        kind: node.kind.clone(),
        children: children.iter().map(|child| child.id).collect(),
    });

    for child in children {
        collect_plan(child, visited, out);
    }
}

#[allow(dead_code)]
fn normalize_plan(
    node_id: usize,
    nodes_by_id: &HashMap<usize, &StreamPlanNode>,
    old_to_new: &mut HashMap<usize, usize>,
    out: &mut Vec<StreamPlanNode>,
) -> usize {
    if let Some(existing) = old_to_new.get(&node_id) {
        return *existing;
    }

    let node = nodes_by_id
        .get(&node_id)
        .copied()
        .expect("normalized plan node must exist");
    let normalized_id = out.len();
    old_to_new.insert(node_id, normalized_id);
    out.push(StreamPlanNode {
        id: normalized_id,
        kind: node.kind.clone(),
        children: Vec::new(),
    });

    let normalized_children = node
        .children
        .iter()
        .map(|child| normalize_plan(*child, nodes_by_id, old_to_new, out))
        .collect();
    out[normalized_id].children = normalized_children;
    normalized_id
}

#[allow(dead_code)]
enum PlanChildren {
    Static(Vec<Arc<PlanNodeRef>>),
    Deferred(Arc<OnceLock<Vec<Arc<PlanNodeRef>>>>),
}

#[allow(dead_code)]
struct PlanNodeRef {
    id: NodeId,
    kind: StreamNodeKind,
    children: PlanChildren,
}

impl PlanNodeRef {
    fn new_static(id: NodeId, kind: StreamNodeKind, children: Vec<Arc<PlanNodeRef>>) -> Arc<Self> {
        Arc::new(Self {
            id,
            kind,
            children: PlanChildren::Static(children),
        })
    }

    fn new_deferred(
        id: NodeId,
        kind: StreamNodeKind,
    ) -> (Arc<Self>, Arc<OnceLock<Vec<Arc<PlanNodeRef>>>>) {
        let deferred = Arc::new(OnceLock::new());
        (
            Arc::new(Self {
                id,
                kind,
                children: PlanChildren::Deferred(deferred.clone()),
            }),
            deferred,
        )
    }

    fn children(&self) -> Vec<Arc<PlanNodeRef>> {
        match &self.children {
            PlanChildren::Static(children) => children.clone(),
            PlanChildren::Deferred(children) => children
                .get()
                .cloned()
                .expect("semantic plan children must be initialized before inspection"),
        }
    }

    fn label(&self) -> String {
        match &self.kind {
            StreamNodeKind::Constant => "constant".to_string(),
            StreamNodeKind::Prefix => "prefix".to_string(),
            StreamNodeKind::Source { name }
            | StreamNodeKind::Input { name }
            | StreamNodeKind::Lift { name }
            | StreamNodeKind::Zip { name }
            | StreamNodeKind::Feedback { name } => name.to_string(),
            StreamNodeKind::Delay => "delay".to_string(),
            StreamNodeKind::BindInput { input } => format!("bind({input})"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InputBinding {
    id: InputId,
    name: Arc<str>,
}

impl InputBinding {
    fn new(name: Arc<str>) -> Self {
        Self {
            id: next_input_id(),
            name,
        }
    }
}

struct BindingFrame {
    bind_node_id: NodeId,
    input_id: InputId,
    stream: Arc<dyn Any + Send + Sync>,
}

#[derive(Default)]
struct EvaluationContext {
    cache: HashMap<CacheKey, Box<dyn Any + Send + Sync>>,
    computing: HashSet<CacheKey>,
    bindings: Vec<BindingFrame>,
}

impl EvaluationContext {
    fn evaluate<T>(&mut self, stream: &Stream<T>, t: usize) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        let key = (
            stream.inner.id,
            self.bindings
                .iter()
                .map(|frame| frame.bind_node_id)
                .collect::<Vec<_>>(),
            t,
        );

        if let Some(value) = self.cache.get(&key) {
            return value
                .downcast_ref::<T>()
                .expect("semantic evaluator cache must preserve value types")
                .clone();
        }

        assert!(
            self.computing.insert(key.clone()),
            "unguarded semantic feedback detected while evaluating '{}' at logical time {}",
            stream.inner.plan.label(),
            t
        );
        let value = stream.inner.node.eval(t, self);
        self.computing.remove(&key);
        self.cache.insert(key, Box::new(value.clone()));
        value
    }

    fn with_binding<T, R>(
        &mut self,
        bind_node_id: NodeId,
        binding: &InputBinding,
        stream: &Stream<T>,
        evaluator: impl FnOnce(&mut Self) -> R,
    ) -> R
    where
        T: Clone + Send + Sync + 'static,
    {
        self.bindings.push(BindingFrame {
            bind_node_id,
            input_id: binding.id,
            stream: Arc::new(stream.clone()),
        });
        let result = evaluator(self);
        self.bindings.pop();
        result
    }

    fn resolve_input<T>(&self, binding: &InputBinding) -> Stream<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.bindings
            .iter()
            .rev()
            .find_map(|frame| {
                if frame.input_id != binding.id {
                    return None;
                }
                Arc::downcast::<Stream<T>>(frame.stream.clone())
                    .ok()
                    .map(|stream| stream.as_ref().clone())
            })
            .unwrap_or_else(|| panic!("unbound semantic circuit input '{}'", binding.name))
    }
}

trait SemanticNode<T>: Send + Sync {
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> T;
}

struct ConstantNode<T> {
    value: T,
}

impl<T> SemanticNode<T> for ConstantNode<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn eval(&self, _t: usize, _ctx: &mut EvaluationContext) -> T {
        self.value.clone()
    }
}

struct PrefixNode<T> {
    prefix: Vec<T>,
    tail: T,
}

impl<T> SemanticNode<T> for PrefixNode<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, _ctx: &mut EvaluationContext) -> T {
        self.prefix
            .get(t)
            .cloned()
            .unwrap_or_else(|| self.tail.clone())
    }
}

struct SourceNode<T> {
    function: Arc<dyn Fn(usize) -> T + Send + Sync>,
}

impl<T> SemanticNode<T> for SourceNode<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, _ctx: &mut EvaluationContext) -> T {
        (self.function)(t)
    }
}

struct InputNode<T> {
    binding: InputBinding,
    marker: PhantomData<T>,
}

impl<T> SemanticNode<T> for InputNode<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> T {
        let input = ctx.resolve_input::<T>(&self.binding);
        ctx.evaluate(&input, t)
    }
}

struct LiftNode<I, O> {
    input: Stream<I>,
    function: Arc<dyn Fn(&I) -> O + Send + Sync>,
}

impl<I, O> SemanticNode<O> for LiftNode<I, O>
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> O {
        let input = ctx.evaluate(&self.input, t);
        (self.function)(&input)
    }
}

struct ZipNode<L, R, O> {
    left: Stream<L>,
    right: Stream<R>,
    function: Arc<dyn Fn(&L, &R) -> O + Send + Sync>,
}

impl<L, R, O> SemanticNode<O> for ZipNode<L, R, O>
where
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> O {
        let left = ctx.evaluate(&self.left, t);
        let right = ctx.evaluate(&self.right, t);
        (self.function)(&left, &right)
    }
}

struct DelayNode<T> {
    input: Stream<T>,
}

impl<T> SemanticNode<T> for DelayNode<T>
where
    T: ZeroValue,
{
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> T {
        if t == 0 {
            T::zero()
        } else {
            ctx.evaluate(&self.input, t - 1)
        }
    }
}

struct FeedbackNode<T> {
    body: OnceLock<Stream<T>>,
}

impl<T> FeedbackNode<T> {
    fn new() -> Self {
        Self {
            body: OnceLock::new(),
        }
    }

    fn set_body(&self, body: Stream<T>) {
        assert!(
            self.body.set(body).is_ok(),
            "semantic feedback body already initialized"
        );
    }
}

impl<T> SemanticNode<T> for FeedbackNode<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> T {
        let body = self
            .body
            .get()
            .expect("semantic feedback body must be initialized before evaluation");
        ctx.evaluate(body, t)
    }
}

struct BindInputNode<T, I> {
    bind_node_id: NodeId,
    binding: InputBinding,
    body: Stream<T>,
    input: Stream<I>,
}

impl<T, I> SemanticNode<T> for BindInputNode<T, I>
where
    T: Clone + Send + Sync + 'static,
    I: Clone + Send + Sync + 'static,
{
    fn eval(&self, t: usize, ctx: &mut EvaluationContext) -> T {
        ctx.with_binding(self.bind_node_id, &self.binding, &self.input, |ctx| {
            ctx.evaluate(&self.body, t)
        })
    }
}

struct StreamInner<T> {
    id: NodeId,
    node: Arc<dyn SemanticNode<T>>,
    plan: Arc<PlanNodeRef>,
}

#[derive(Clone)]
pub struct Stream<T> {
    inner: Arc<StreamInner<T>>,
}

impl<T> Stream<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn from_parts(
        id: NodeId,
        node: impl SemanticNode<T> + 'static,
        plan: Arc<PlanNodeRef>,
    ) -> Self {
        Self {
            inner: Arc::new(StreamInner {
                id,
                node: Arc::new(node),
                plan,
            }),
        }
    }

    fn from_static_node(
        id: NodeId,
        kind: StreamNodeKind,
        children: Vec<Arc<PlanNodeRef>>,
        node: impl SemanticNode<T> + 'static,
    ) -> Self {
        Self::from_parts(id, node, PlanNodeRef::new_static(id, kind, children))
    }

    pub(crate) fn input_placeholder(name: impl Into<Arc<str>>) -> (Self, InputBinding) {
        let name = name.into();
        let binding = InputBinding::new(name.clone());
        let id = next_node_id();
        let stream = Self::from_static_node(
            id,
            StreamNodeKind::Input { name },
            Vec::new(),
            InputNode::<T> {
                binding: binding.clone(),
                marker: PhantomData,
            },
        );
        (stream, binding)
    }

    pub(crate) fn bind_input<I>(&self, binding: InputBinding, input: Stream<I>) -> Self
    where
        I: Clone + Send + Sync + 'static,
    {
        let id = next_node_id();
        let plan = PlanNodeRef::new_static(
            id,
            StreamNodeKind::BindInput {
                input: binding.name.clone(),
            },
            vec![self.inner.plan.clone(), input.inner.plan.clone()],
        );
        Self::from_parts(
            id,
            BindInputNode {
                bind_node_id: id,
                binding,
                body: self.clone(),
                input,
            },
            plan,
        )
    }

    #[allow(dead_code)]
    #[allow(dead_code)]
    pub(crate) fn plan(&self) -> StreamPlan {
        StreamPlan::from_root(self.inner.plan.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn normalized_plan(&self) -> StreamPlan {
        self.plan().normalized()
    }

    pub fn from_fn(
        name: &'static str,
        function: impl Fn(usize) -> T + Send + Sync + 'static,
    ) -> Self {
        let id = next_node_id();
        let name = Arc::<str>::from(name);
        Self::from_static_node(
            id,
            StreamNodeKind::Source { name },
            Vec::new(),
            SourceNode {
                function: Arc::new(function),
            },
        )
    }

    pub fn constant(value: T) -> Self {
        let id = next_node_id();
        Self::from_static_node(
            id,
            StreamNodeKind::Constant,
            Vec::new(),
            ConstantNode { value },
        )
    }

    pub fn from_prefix(prefix: impl Into<Vec<T>>, tail: T) -> Self {
        let id = next_node_id();
        Self::from_static_node(
            id,
            StreamNodeKind::Prefix,
            Vec::new(),
            PrefixNode {
                prefix: prefix.into(),
                tail,
            },
        )
    }

    pub fn at(&self, t: usize) -> T {
        let mut evaluator = ReferenceEvaluator::default();
        evaluator.at(self, t)
    }

    pub fn prefix(&self, len: usize) -> Vec<T> {
        let mut evaluator = ReferenceEvaluator::default();
        (0..len).map(|t| evaluator.at(self, t)).collect()
    }

    fn lift_with<U>(
        &self,
        name: Arc<str>,
        function: Arc<dyn Fn(&T) -> U + Send + Sync>,
    ) -> Stream<U>
    where
        U: Clone + Send + Sync + 'static,
    {
        let id = next_node_id();
        Stream::from_static_node(
            id,
            StreamNodeKind::Lift { name },
            vec![self.inner.plan.clone()],
            LiftNode {
                input: self.clone(),
                function,
            },
        )
    }

    pub fn lift<U>(
        &self,
        name: &'static str,
        function: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> Stream<U>
    where
        U: Clone + Send + Sync + 'static,
    {
        self.lift_with(Arc::<str>::from(name), Arc::new(function))
    }
}

pub fn zip_with<L, R, O>(
    name: &'static str,
    left: &Stream<L>,
    right: &Stream<R>,
    function: impl Fn(&L, &R) -> O + Send + Sync + 'static,
) -> Stream<O>
where
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    let id = next_node_id();
    Stream::from_static_node(
        id,
        StreamNodeKind::Zip {
            name: Arc::<str>::from(name),
        },
        vec![left.inner.plan.clone(), right.inner.plan.clone()],
        ZipNode {
            left: left.clone(),
            right: right.clone(),
            function: Arc::new(function),
        },
    )
}

pub fn pair<L, R>(left: &Stream<L>, right: &Stream<R>) -> Stream<(L, R)>
where
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    zip_with("pair", left, right, |l, r| (l.clone(), r.clone()))
}

pub fn add<T>(left: &Stream<T>, right: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    zip_with("add", left, right, |l, r| l.add(r))
}

pub fn negate<T>(input: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    input.lift("negate", |value| value.neg())
}

pub fn subtract<T>(left: &Stream<T>, right: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    zip_with("subtract", left, right, |l, r| l.sub(r))
}

pub fn delay<T>(input: &Stream<T>) -> Stream<T>
where
    T: ZeroValue,
{
    let id = next_node_id();
    Stream::from_static_node(
        id,
        StreamNodeKind::Delay,
        vec![input.inner.plan.clone()],
        DelayNode {
            input: input.clone(),
        },
    )
}

pub fn differentiate<T>(input: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    subtract(input, &delay(input))
}

pub fn feedback<T>(
    name: &'static str,
    builder: impl Fn(Stream<T>) -> Stream<T> + Send + Sync + 'static,
) -> Stream<T>
where
    T: Clone + Send + Sync + 'static,
{
    let id = next_node_id();
    let (plan, plan_children) = PlanNodeRef::new_deferred(
        id,
        StreamNodeKind::Feedback {
            name: Arc::<str>::from(name),
        },
    );
    let node = Arc::new(FeedbackNode::new());
    let stream = Stream {
        inner: Arc::new(StreamInner {
            id,
            node: node.clone(),
            plan,
        }),
    };
    let body = builder(stream.clone());
    node.set_body(body.clone());
    assert!(
        plan_children.set(vec![body.inner.plan.clone()]).is_ok(),
        "semantic feedback plan already initialized"
    );
    stream
}

pub fn integrate<T>(input: &Stream<T>) -> Stream<T>
where
    T: GroupValue,
{
    let input = input.clone();
    feedback("integrate", move |state| add(&input, &delay(&state)))
}

#[derive(Default)]
pub struct ReferenceEvaluator {
    context: EvaluationContext,
}

impl ReferenceEvaluator {
    pub fn at<T>(&mut self, stream: &Stream<T>, t: usize) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        self.context.evaluate(stream, t)
    }

    pub fn observe_prefix<T>(stream: &Stream<T>, len: usize) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut evaluator = Self::default();
        (0..len).map(|t| evaluator.at(stream, t)).collect()
    }
}
