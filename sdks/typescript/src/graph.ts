import { Projection, QueryRequest, Traversal, readBatch } from "./dsl.js";

const PRIVATE_PREFIX = "__helix_graph_";
const NODE_ID = "__helix_graph_node_id";
const EXTERNAL_ID = "__helix_graph_external_id";
const NODE_LABEL = "__helix_graph_node_label";
const EDGE_ID = "__helix_graph_edge_id";
const EDGE_KEY = "__helix_graph_edge_key";
const EDGE_SOURCE = "__helix_graph_edge_source";
const EDGE_TARGET = "__helix_graph_edge_target";
const EDGE_LABEL = "__helix_graph_edge_label";
const EDGE_WEIGHT = "__helix_graph_edge_weight";

export type GraphDirection = "directed" | "undirected";
export type TraversalDirection = "out" | "in" | "both";
export type DegreeKind = "in" | "out" | "total";

export interface GraphSelectionOptions {
  nodeTraversal: Traversal<any, any>;
  edgeTraversal: Traversal<any, any>;
  direction?: GraphDirection;
  nodeProperties?: readonly string[];
  edgeProperties?: readonly string[];
  externalIdentityProperty?: string;
  graphifyEdgeKeyProperty?: string;
  weightProperty?: string;
  maxNodes?: number;
  maxEdges?: number;
  allowFullScan?: boolean;
}

/** Typed inputs for the one ordinary graph-loading read batch. */
export class GraphSelection {
  readonly nodeTraversal: Traversal<any, any>;
  readonly edgeTraversal: Traversal<any, any>;
  readonly direction: GraphDirection;
  readonly nodeProperties: readonly string[];
  readonly edgeProperties: readonly string[];
  readonly externalIdentityProperty?: string;
  readonly graphifyEdgeKeyProperty?: string;
  readonly weightProperty?: string;
  readonly maxNodes?: number;
  readonly maxEdges?: number;
  readonly allowFullScan: boolean;

  constructor(options: GraphSelectionOptions) {
    this.nodeTraversal = options.nodeTraversal;
    this.edgeTraversal = options.edgeTraversal;
    this.direction = options.direction ?? "directed";
    this.nodeProperties = [...new Set(options.nodeProperties ?? [])].sort();
    this.edgeProperties = [...new Set(options.edgeProperties ?? [])].sort();
    this.externalIdentityProperty = options.externalIdentityProperty;
    this.graphifyEdgeKeyProperty = options.graphifyEdgeKeyProperty;
    this.weightProperty = options.weightProperty;
    this.maxNodes = positiveInteger(options.maxNodes, "maxNodes");
    this.maxEdges = positiveInteger(options.maxEdges, "maxEdges");
    this.allowFullScan = options.allowFullScan ?? false;
    if (this.direction !== "directed" && this.direction !== "undirected") {
      throw new TypeError("direction must be 'directed' or 'undirected'");
    }
    for (const property of [
      ...this.nodeProperties,
      ...this.edgeProperties,
      this.externalIdentityProperty,
      this.graphifyEdgeKeyProperty,
      this.weightProperty,
    ]) {
      if (property === undefined) continue;
      if (property.length === 0) throw new TypeError("graph property names must not be empty");
      if (property.startsWith(PRIVATE_PREFIX)) throw new TypeError(`graph property uses reserved prefix: ${property}`);
    }
    const startsWithFullScan = (traversal: Traversal<any, any>): boolean => {
      const [source] = traversal.intoSteps();
      if (source === undefined || (source.variant !== "N" && source.variant !== "E")) return false;
      return (source.payload as { variant?: string } | undefined)?.variant === "All";
    };
    if (!this.allowFullScan && (startsWithFullScan(this.nodeTraversal) || startsWithFullScan(this.edgeTraversal))) {
      throw new TypeError("full graph scans require allowFullScan: true");
    }
  }

  toQueryRequest(): QueryRequest {
    const nodeProjection = [
      Projection.property("$id", NODE_ID),
      Projection.property(this.externalIdentityProperty ?? "$id", EXTERNAL_ID),
      Projection.property("$label", NODE_LABEL),
      ...this.nodeProperties.map((property) => Projection.property(property, property)),
    ];
    const edgeProjection = [
      Projection.property("$id", EDGE_ID),
      Projection.fromEndpoint("$id", EDGE_SOURCE),
      Projection.toEndpoint("$id", EDGE_TARGET),
      Projection.property("$label", EDGE_LABEL),
    ];
    if (this.graphifyEdgeKeyProperty !== undefined) edgeProjection.push(Projection.property(this.graphifyEdgeKeyProperty, EDGE_KEY));
    if (this.weightProperty !== undefined) edgeProjection.push(Projection.property(this.weightProperty, EDGE_WEIGHT));
    edgeProjection.push(...this.edgeProperties.map((property) => Projection.property(property, property)));
    const nodes = this.maxNodes === undefined ? this.nodeTraversal : this.nodeTraversal.limit(this.maxNodes + 1);
    const edges = this.maxEdges === undefined ? this.edgeTraversal : this.edgeTraversal.limit(this.maxEdges + 1);
    return readBatch()
      .varAs("nodes", nodes.project(nodeProjection))
      .varAs("edges", edges.project(edgeProjection))
      .returning(["nodes", "edges"])
      .toQueryRequest();
  }
}

export interface GraphNode {
  id: string;
  label?: string;
  attributes: Readonly<Record<string, unknown>>;
}

export interface GraphEdge {
  id: string;
  graphifyKey?: string;
  source: string;
  target: string;
  label?: string;
  weight?: number;
  attributes: Readonly<Record<string, unknown>>;
}

export interface BetweennessOptions {
  mode?: "exact" | "sampled" | "auto";
  sampleCount?: number;
  seed?: number;
  exactThrough?: number;
  normalized?: boolean;
  endpoints?: boolean;
  weighted?: boolean;
}

export interface TraversalOptions {
  seeds: readonly string[];
  maxDepth: number;
  strategy?: "breadthFirst" | "depthFirst";
  direction?: TraversalDirection;
  allowedLabels?: readonly string[];
  stopNonSeedAtOrAboveDegree?: number;
}

export interface LouvainOptions {
  resolution?: number;
  threshold?: number;
  seed?: number;
  maxLevels?: number;
}

export interface LayoutOptions {
  k?: number;
  iterations?: number;
  seed?: number;
  weighted?: boolean;
  initialPositions?: readonly { nodeId: string; x: number; y: number }[];
}

type NativeRecord = Record<string, any>;
type NativeGraphHandle = NativeRecord;

export interface NativeGraphModule extends NativeRecord {
  graph_from_query_response(spec: NativeRecord, response: Uint8Array): NativeGraphHandle;
}

export interface GraphLoadingClient {
  _graphResponse(request: QueryRequest, spec: NativeRecord): Promise<Uint8Array | NativeGraphHandle>;
}

/** Raised only when `client.graph(...)` is used without native bindings. */
export class NativeGraphUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NativeGraphUnavailable";
  }
}

/** Immutable graph whose accessors and algorithms all execute in Rust. */
export class NativeGraph {
  constructor(
    private readonly native: NativeGraphHandle,
    private readonly module: NativeGraphModule,
  ) {}

  get nodeCount(): number {
    return Number(this.native.node_count());
  }

  get edgeCount(): number {
    return Number(this.native.edge_count());
  }

  get directed(): boolean {
    return this.native.is_directed();
  }

  get multigraph(): boolean {
    return this.native.is_multigraph();
  }

  get attributes(): Readonly<Record<string, unknown>> {
    return decodeJson(this.native.graph_attributes_json());
  }

  containsNode(nodeId: string): boolean {
    return this.native.contains_node(nodeId);
  }

  containsEdge(edgeId: string): boolean {
    return this.native.contains_edge(edgeId);
  }

  nodes(): readonly GraphNode[] {
    return this.native.nodes().map(nodeRecord);
  }

  edges(): readonly GraphEdge[] {
    return this.native.edges().map(edgeRecord);
  }

  node(nodeId: string): GraphNode | undefined {
    const record = this.native.node(nodeId);
    return record === undefined ? undefined : nodeRecord(record);
  }

  edge(edgeId: string): GraphEdge | undefined {
    const record = this.native.edge(edgeId);
    return record === undefined ? undefined : edgeRecord(record);
  }

  neighbors(nodeId: string, direction: TraversalDirection = "both"): readonly string[] {
    return this.native.neighbors(nodeId, nativeDirection(this.module, direction));
  }

  successors(nodeId: string): readonly string[] {
    return this.native.successors(nodeId);
  }

  predecessors(nodeId: string): readonly string[] {
    return this.native.predecessors(nodeId);
  }

  outEdgeIds(nodeId: string): readonly string[] {
    return this.native.out_edge_ids(nodeId);
  }

  inEdgeIds(nodeId: string): readonly string[] {
    return this.native.in_edge_ids(nodeId);
  }

  incidentEdgeIds(nodeId: string): readonly string[] {
    return this.native.incident_edge_ids(nodeId);
  }

  edgesBetween(source: string, target: string, direction: TraversalDirection = "both"): readonly string[] {
    return this.native.edges_between(source, target, nativeDirection(this.module, direction));
  }

  hasEdgeBetween(source: string, target: string, direction: TraversalDirection = "both"): boolean {
    return this.native.has_edge_between(source, target, nativeDirection(this.module, direction));
  }

  degree(nodeId: string, kind: DegreeKind = "total"): NativeRecord {
    return this.native.degree(nodeId, nativeDegreeKind(this.module, kind));
  }

  degrees(kind: DegreeKind = "total"): readonly NativeRecord[] {
    return this.native.degrees(nativeDegreeKind(this.module, kind));
  }

  async betweennessCentrality(options: BetweennessOptions = {}): Promise<readonly NativeRecord[]> {
    return this.native.betweenness_centrality_async(nativeBetweennessOptions(this.module, options));
  }

  async edgeBetweennessCentrality(options: BetweennessOptions = {}): Promise<readonly NativeRecord[]> {
    return this.native.edge_betweenness_centrality_async(nativeBetweennessOptions(this.module, options));
  }

  async simpleCycles(lengthBound: number, maxCycles?: number): Promise<NativeRecord> {
    return this.native.simple_cycles_async(lengthBound, maxCycles);
  }

  async traverse(options: TraversalOptions): Promise<NativeRecord> {
    const strategy =
      (options.strategy ?? "breadthFirst") === "breadthFirst"
        ? this.module.NativeTraversalStrategy.BreadthFirst
        : this.module.NativeTraversalStrategy.DepthFirst;
    const hub =
      options.stopNonSeedAtOrAboveDegree === undefined
        ? this.module.NativeHubExpansionPolicy.ExpandAll()
        : this.module.NativeHubExpansionPolicy.StopNonSeedAtOrAbove(options.stopNonSeedAtOrAboveDegree);
    return this.native.traverse_async({
      strategy,
      seeds: [...options.seeds],
      max_depth: options.maxDepth,
      direction: nativeDirection(this.module, options.direction ?? "both"),
      allowed_labels: [...(options.allowedLabels ?? [])],
      hub_policy: hub,
    });
  }

  async shortestPath(
    source: string,
    target: string,
    options: { direction?: TraversalDirection; allowedLabels?: readonly string[]; maxDepth?: number } = {},
  ): Promise<NativeRecord> {
    return this.native.shortest_path_async(
      source,
      target,
      nativeDirection(this.module, options.direction ?? "both"),
      [...(options.allowedLabels ?? [])],
      options.maxDepth,
    );
  }

  async louvainCommunities(options: LouvainOptions = {}): Promise<NativeRecord> {
    return this.native.louvain_communities_async(
      options.resolution ?? 1,
      options.threshold ?? 1e-4,
      options.seed ?? 42,
      options.maxLevels ?? 10,
    );
  }

  async springLayout(options: LayoutOptions = {}): Promise<readonly NativeRecord[]> {
    return this.native.spring_layout_async(
      options.k,
      options.iterations ?? 50,
      options.seed ?? 42,
      options.weighted ?? true,
      (options.initialPositions ?? []).map((position) => ({ node_id: position.nodeId, x: position.x, y: position.y })),
    );
  }

  inducedSubgraph(nodeIds: readonly string[]): NativeGraph {
    return new NativeGraph(this.native.induced_subgraph([...nodeIds]), this.module);
  }

  toUndirected(): NativeGraph {
    return new NativeGraph(this.native.to_undirected(), this.module);
  }

  copy(): NativeGraph {
    return new NativeGraph(this.native.copy(), this.module);
  }

  compose(right: NativeGraph): NativeGraph {
    if (this.module !== right.module) throw new TypeError("graphs must originate from the same native binding module");
    return new NativeGraph(this.native.compose(right.native), this.module);
  }

  relabel(mapping: Readonly<Record<string, string>> | ReadonlyMap<string, string>): NativeGraph {
    const entries = mapping instanceof Map ? [...mapping] : Object.entries(mapping);
    return new NativeGraph(this.native.relabel(entries.map(([from, to]) => ({ from, to }))), this.module);
  }
}

export async function loadGraph(client: GraphLoadingClient, selection: GraphSelection): Promise<NativeGraph> {
  const native = await loadNativeGraphModule();
  const spec = {
    direction: selection.direction === "directed" ? native.NativeGraphDirection.Directed : native.NativeGraphDirection.Undirected,
    node_limit: selection.maxNodes,
    edge_limit: selection.maxEdges,
  };
  const response = await client._graphResponse(selection.toQueryRequest(), spec);
  return new NativeGraph(response instanceof Uint8Array ? native.graph_from_query_response(spec, response) : response, native);
}

const DEFAULT_NATIVE_PACKAGE = "@helix-db/uniffi";
const dynamicImport = new Function("specifier", "return import(specifier)") as (specifier: string) => Promise<NativeGraphModule>;

async function loadNativeGraphModule(): Promise<NativeGraphModule> {
  const packageName = process.env.HELIXDB_UNIFFI_NODE_PACKAGE ?? DEFAULT_NATIVE_PACKAGE;
  let native: NativeGraphModule;
  try {
    native = await dynamicImport(packageName);
  } catch (error) {
    throw new NativeGraphUnavailable(`native graph bindings unavailable: ${error instanceof Error ? error.message : String(error)}`);
  }
  for (const name of ["NativeGraphDirection", "NativeTraversalDirection", "graph_from_query_response"]) {
    if (native[name] === undefined) throw new NativeGraphUnavailable(`${packageName} does not export ${name}`);
  }
  return native;
}

function nodeRecord(record: NativeRecord): GraphNode {
  return { id: record.id, label: record.label, attributes: decodeJson(record.attributes_json) };
}

function edgeRecord(record: NativeRecord): GraphEdge {
  return {
    id: record.id,
    graphifyKey: record.graphify_key,
    source: record.source,
    target: record.target,
    label: record.label,
    weight: record.weight,
    attributes: decodeJson(record.attributes_json),
  };
}

function decodeJson(bytes: Uint8Array): Readonly<Record<string, unknown>> {
  return JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
}

function nativeDirection(native: NativeGraphModule, direction: TraversalDirection): unknown {
  const values = {
    out: native.NativeTraversalDirection.Out,
    in: native.NativeTraversalDirection.In,
    both: native.NativeTraversalDirection.Both,
  };
  return values[direction];
}

function nativeDegreeKind(native: NativeGraphModule, kind: DegreeKind): unknown {
  const values = { in: native.NativeDegreeKind.In, out: native.NativeDegreeKind.Out, total: native.NativeDegreeKind.Total };
  return values[kind];
}

function nativeBetweennessOptions(native: NativeGraphModule, options: BetweennessOptions): NativeRecord {
  const sampleCount = positiveInteger(options.sampleCount ?? 100, "sampleCount")!;
  const seed = options.seed ?? 42;
  const exactThrough = nonNegativeInteger(options.exactThrough ?? 1_000, "exactThrough");
  const mode = options.mode ?? "exact";
  return {
    mode:
      mode === "exact"
        ? native.NativeBetweennessMode.Exact()
        : mode === "sampled"
          ? native.NativeBetweennessMode.Sampled(sampleCount, seed)
          : native.NativeBetweennessMode.Auto(exactThrough, sampleCount, seed),
    normalized: options.normalized ?? true,
    endpoints: options.endpoints ?? false,
    weighted: options.weighted ?? false,
  };
}

function positiveInteger(value: number | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`${name} must be a positive safe integer`);
  return value;
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a non-negative safe integer`);
  return value;
}

export { EDGE_SOURCE, EXTERNAL_ID };
