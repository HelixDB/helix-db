export const ExprKindString = { kind: "String" as const };
export const ExprKindI64 = { kind: "I64" as const };
export const ExprKindF64 = { kind: "F64" as const };
export const ExprKindBoolean = { kind: "Boolean" as const };
export const ExprKindVector = { kind: "Vector" as const };
export const ExprKindList = (item: ExprKind) => ({
  kind: "List" as const,
  item,
});
export const ExprKindStruct = (fields: Record<string, ExprKind>) => ({
  kind: "Struct" as const,
  fields,
});

export type ExprKind =
  | typeof ExprKindString
  | typeof ExprKindI64
  | typeof ExprKindF64
  | typeof ExprKindBoolean
  | typeof ExprKindVector
  | { kind: "List"; item: ExprKind }
  | { kind: "Struct"; fields: Record<string, ExprKind> };

export type Expr = {
  kind: ExprKind;
  expr:
    | { expr: "Argument"; index: number }
    | { expr: "PropAccess"; target: Expr; field: string }
    | { expr: "Literal"; value: any }
    | { expr: "BinaryOp"; op: string; left: Expr; right: Expr }
    | { expr: "Call"; func: string; args: Expr[] };
};

export type Statement =
  | { stmt: "Expr"; expr: Expr }
  | { stmt: "Return"; value: Expr };

export type Block = Statement[];

export type Query = {
  name: string;
  arguments: ExprKind[];
  returns: ExprKind;
  body: Block;
};

export type NodeName = `node_${number}`;
export type Node = {
  id: typeof ExprKindI64;
  [_: string]: ExprKind;
};

export type EdgeName = `edge_${number}`;
export type Edge = {
  id: typeof ExprKindI64;
  from: NodeName;
  to: NodeName;
};

export const GlobalVectorspaceName = `vectorspace_global` as const;
export type GlobalVectorspaceName = typeof GlobalVectorspaceName;
export type GlobalVectorspace = symbol;

export type VectorspaceName = `vectorspace_${number}`;
export type Vectorspace = {
  dimensions: number;
  hnsw: any; // TODO
};

export type Schema = {
  nodes: Record<NodeName, Node>;
  indices: [{ on: NodeName; field: string; unique: boolean }];

  edges: Record<EdgeName, Edge>;

  vectorspaces:
    & Record<GlobalVectorspaceName, GlobalVectorspace>
    & Record<VectorspaceName, Vectorspace>;
};
