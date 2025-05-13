use std::fmt::{self, Display};

pub enum GenRef<T>
where
    T: Display,
{
    Literal(T),
    Mut(T),
    Ref(T),
    RefLT(String, T),
    DeRef(T),
    MutRef(T),
    MutRefLT(String, T),
    MutDeRef(T),
    RefLiteral(T),
}

impl<T> Display for GenRef<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GenRef::Literal(t) => write!(f, "{}", t),
            GenRef::Mut(t) => write!(f, "mut {}", t),
            GenRef::Ref(t) => write!(f, "&{}", t),
            GenRef::RefLT(lifetime_name, t) => write!(f, "&'{} {}", lifetime_name, t),
            GenRef::DeRef(t) => write!(f, "*{}", t),
            GenRef::MutRef(t) => write!(f, "& mut {}", t),
            GenRef::MutRefLT(lifetime_name, t) => write!(f, "&'{} mut {}", lifetime_name, t),
            GenRef::MutDeRef(t) => write!(f, "mut *{}", t),
            GenRef::RefLiteral(t) => write!(f, "ref {}", t),
        }
    }
}

impl<T> GenRef<T>
where
    T: Display,
{
    pub fn inner(&self) -> &T {
        match self {
            GenRef::Literal(t) => t,
            GenRef::Mut(t) => t,
            GenRef::Ref(t) => t,
            GenRef::RefLT(_, t) => t,
            GenRef::DeRef(t) => t,
            GenRef::MutRef(t) => t,
            GenRef::MutRefLT(_, t) => t,
            GenRef::MutDeRef(t) => t,
            GenRef::RefLiteral(t) => t,
        }
    }
}
