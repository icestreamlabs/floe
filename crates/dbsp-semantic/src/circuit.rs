use std::sync::Arc;

use crate::stream::{Stream, add, delay, differentiate, integrate, pair};
use crate::values::{GroupValue, ZeroValue};

#[derive(Clone)]
pub struct Circuit<I, O> {
    name: Arc<str>,
    function: Arc<dyn Fn(Stream<I>) -> Stream<O> + Send + Sync>,
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
        Self {
            name: name.into(),
            function: Arc::new(function),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn apply(&self, input: Stream<I>) -> Stream<O> {
        (self.function)(input)
    }

    pub fn compose<M>(&self, next: Circuit<O, M>) -> Circuit<I, M>
    where
        M: Clone + Send + Sync + 'static,
    {
        let left = self.clone();
        let right = next.clone();
        Circuit::new(
            format!("{} -> {}", self.name(), next.name()),
            move |input| {
                let intermediate = left.apply(input);
                right.apply(intermediate)
            },
        )
    }

    pub fn fanout<P>(&self, other: Circuit<I, P>) -> Circuit<I, (O, P)>
    where
        P: Clone + Send + Sync + 'static,
    {
        let left = self.clone();
        let right = other.clone();
        Circuit::new(
            format!("{} || {}", self.name(), other.name()),
            move |input| {
                let left_output = left.apply(input.clone());
                let right_output = right.apply(input);
                pair(&left_output, &right_output)
            },
        )
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
    Circuit::new(format!("D({})", circuit.name()), move |input| {
        let output = circuit.apply(input);
        differentiate(&output)
    })
}

pub fn circuit_i<I, O>(circuit: Circuit<I, O>) -> Circuit<I, O>
where
    I: GroupValue,
    O: GroupValue,
{
    Circuit::new(format!("I({})", circuit.name()), move |input| {
        let output = circuit.apply(input);
        integrate(&output)
    })
}

pub fn incrementalize<I, O>(query: Circuit<I, O>) -> Circuit<I, O>
where
    I: GroupValue,
    O: GroupValue,
{
    Circuit::new(
        format!("D(o)↑Q(o)I({})", query.name()),
        move |delta_input| {
            let integrated_input = integrate(&delta_input);
            let query_output = query.apply(integrated_input);
            differentiate(&query_output)
        },
    )
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
