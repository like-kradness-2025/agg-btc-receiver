/**
 * The private routes of a module, kept for the wiring that opens it.
 *
 * A module's unguarded routes must not sit on the object the module returns, and must not be handed to it
 * as an argument either: whoever can reach one can hand the same frame to the same module twice, or release
 * the right that is holding its own frame, and an argument object is something a caller keeps. Only the
 * wiring imports this module; nothing here is on, or in, what a caller is handed.
 */
const INTERNALS = new WeakMap();

/** Called by the module itself, on the object it returns. */
export function bindInternals(module, internal) {
  INTERNALS.set(module, internal);
}

/** Called by the wiring, on the module it opened. */
export function internalsOf(module) {
  const internal = INTERNALS.get(module);
  if (!internal) throw new TypeError('this module was not opened by the wiring, so it has no private routes');
  return internal;
}

/**
 * The internal constructor of a part, for the wiring that builds the structure.
 *
 * The public constructor takes the execution right for the whole of its initialisation. A structure that
 * builds its parts inside an operation it already holds must not take it again, so the part registers the
 * function that does the work here, and only the wiring can ask for it. Nothing a caller is handed reaches
 * either name.
 */
const CONSTRUCTORS = new Map();

export function bindConstructor(name, build) {
  CONSTRUCTORS.set(name, build);
}

export function constructorOf(name) {
  const build = CONSTRUCTORS.get(name);
  if (!build) throw new TypeError(`no internal constructor for ${name}: this part was not built by the wiring`);
  return build;
}
