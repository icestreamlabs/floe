use std::marker::PhantomData;
use std::sync::Arc;

use crate::stream::{InputBinding, Stream, StreamPlan, add, delay, differentiate, integrate, pair};
use crate::values::{GroupValue, ZeroValue};

#[derive(Clone)]
pub struct Circuit<I, O> {
    name: Arc<str>,
    input: InputBinding,
    output: Stream<O>,
    marker: PhantomData<fn(I) -> O>,
}

impl<I, O> Circuit<I, O>
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    pub fn new(
        name: impl Into<Arc<str>>,
        function: impl Fn(Stream<I>) -> Stream<O> + Send + Sync + 'static,
    ) -> Self {
        let name = name.into();
        let (input, binding) = Stream::input_placeholder(name.clone());
        let output = function(input);
        Self::from_parts(name, binding, output)
    }

    fn from_parts(name: Arc<str>, input: InputBinding, output: Stream<O>) -> Self {
        Self {
            name,
            input,
            output,
            marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn apply(&self, input: Stream<I>) -> Stream<O> {
        self.output.bind_input(self.input.clone(), input)
    }

    #[allow(dead_code)]
    pub(crate) fn plan(&self) -> StreamPlan {
        self.output.normalized_plan()
    }

    pub fn compose<M>(&self, next: Circuit<O, M>) -> Circuit<I, M>
    where
        M: Clone + Send + Sync + 'static,
    {
        let name = Arc::<str>::from(format!("{} -> {}", self.name(), next.name()));
        let (input, binding) = Stream::input_placeholder(name.clone());
        let output = next.apply(self.apply(input));
        Circuit::from_parts(name, binding, output)
    }

    pub fn fanout<P>(&self, other: Circuit<I, P>) -> Circuit<I, (O, P)>
    where
        P: Clone + Send + Sync + 'static,
    {
        let name = Arc::<str>::from(format!("{} || {}", self.name(), other.name()));
        let (input, binding) = Stream::input_placeholder(name.clone());
        let output = pair(&self.apply(input.clone()), &other.apply(input));
        Circuit::from_parts(name, binding, output)
    }
}

pub fn identity<I>() -> Circuit<I, I>
where
    I: Clone + Send + Sync + 'static,
{
    Circuit::new("identity", |input| input)
}

pub fn pointwise<I, O>(
    name: &'static str,
    function: impl Fn(&I) -> O + Send + Sync + 'static,
) -> Circuit<I, O>
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    let function = Arc::new(function);
    Circuit::new(name, move |input| {
        let function = function.clone();
        input.lift(name, move |value| function(value))
    })
}

pub fn strict_delay<T>() -> Circuit<T, T>
where
    T: ZeroValue,
{
    Circuit::new("delay", |input| delay(&input))
}

pub fn circuit_d<I, O>(circuit: Circuit<I, O>) -> Circuit<I, O>
where
    I: GroupValue,
    O: GroupValue,
{
    let name = Arc::<str>::from(format!("D({})", circuit.name()));
    let (input, binding) = Stream::input_placeholder(name.clone());
    let output = differentiate(&circuit.apply(input));
    Circuit::from_parts(name, binding, output)
}

pub fn circuit_i<I, O>(circuit: Circuit<I, O>) -> Circuit<I, O>
where
    I: GroupValue,
    O: GroupValue,
{
    let name = Arc::<str>::from(format!("I({})", circuit.name()));
    let (input, binding) = Stream::input_placeholder(name.clone());
    let output = integrate(&circuit.apply(input));
    Circuit::from_parts(name, binding, output)
}

pub fn incrementalize<I, O>(query: Circuit<I, O>) -> Circuit<I, O>
where
    I: GroupValue,
    O: GroupValue,
{
    let name = Arc::<str>::from(format!("D(o)↑Q(o)I({})", query.name()));
    let (input, binding) = Stream::input_placeholder(name.clone());
    let integrated = integrate(&input);
    let output = differentiate(&query.apply(integrated));
    Circuit::from_parts(name, binding, output)
}

pub fn add_circuit<T>() -> Circuit<(T, T), T>
where
    T: GroupValue,
{
    Circuit::new("add", move |input| {
        let left = input.lift("fst", |value: &(T, T)| value.0.clone());
        let right = input.lift("snd", |value: &(T, T)| value.1.clone());
        add(&left, &right)
    })
}
