/**
 * The common IPC message shape and its vocabulary (three-process design, docs/fix-plan-sets.md
 * §5.8, rulings ⑥⑪⑫ - Astra).
 *
 * Every message that travels between ingest, organize, book and supervisor is one shape:
 *
 *   {version, type, request_id, role_instance, run_id, market, stream, connection_id, generation, payload}
 *
 * Which of those fields a message must carry depends on its `type`: REQUIRED_FIELDS_BY_TYPE below
 * is the contract, and it is enforced twice - once when a message is encoded for the wire and once
 * when a message is decoded off it. A message missing a required field is refused; it is never
 * passed on with a hole, at either end. `payload` is the message's own body: its shape belongs to
 * the stage that gives the type its meaning, so stage 1 checks only that the field is present and
 * never interprets it (a resend range, a durable ceiling and an applied boundary are all opaque
 * here).
 *
 * This module is pure: it opens no clock, no socket and no file. It is the vocabulary ipc.mjs
 * carries, not a transport.
 */

/** The shape's version. Bumped only when the common form itself changes incompatibly. */
export const IPC_VERSION = 1;

/** The exact order of the common fields, and the full set a message may carry. */
export const COMMON_FIELDS = Object.freeze([
  'version',
  'type',
  'request_id',
  'role_instance',
  'run_id',
  'market',
  'stream',
  'connection_id',
  'generation',
  'payload',
]);

/** Every control message type the three processes speak (ruling ⑥). Nothing else is a message. */
export const MESSAGE_TYPES = Object.freeze([
  'hello', // a role process announces itself and its role instance on connect
  'accept', // ingest accepts an instance explicitly (takeover; the old instance is refused)
  'accepted', // the peer's response to accept
  'durable_ack', // organize: data is durably written up to a contiguous ceiling
  'applied_ack', // book: the board has applied a boundary
  'resend', // ask the peer to resend a range (a request that carries its own id)
  'invalidate', // organize: a loss/range must be invalidated (persisted request)
  'invalidated', // book: serving stopped and the invalidation was persisted (response)
  'stop', // staged stop request; carries request_id so the response can be matched
  'tail_sealed', // ingest: after receive stop, the final tails of all connections
  'drained', // organize: the durable raw ceiling reached the sealed tail
  'stopped', // the completion response to a stop request
  'error', // a role reports a failure
  'readiness', // a role's readiness state (lost on disconnect / generation mismatch / deadline)
]);

const TYPE_SET = new Set(MESSAGE_TYPES);

/**
 * The required-fields table: for each type, the fields it MUST carry. `version`, `type` and
 * `role_instance` are on every type - a message with no sender identity cannot be routed or
 * attributed, and one with no type cannot be understood. Connection-scoped messages add the
 * identity of the stream they concern (run_id / market / stream / connection_id / generation).
 * Paired request/response messages add `request_id` so a response is matched, not guessed.
 * `payload` is required only for the types whose whole purpose is to carry a body.
 *
 * Frozen so the contract cannot be edited by a caller who holds it.
 */
export const REQUIRED_FIELDS_BY_TYPE = Object.freeze({
  hello: Object.freeze(['version', 'type', 'role_instance', 'run_id']),
  accept: Object.freeze([
    'version',
    'type',
    'role_instance',
    'request_id',
    'run_id',
    'connection_id',
    'generation',
  ]),
  accepted: Object.freeze([
    'version',
    'type',
    'role_instance',
    'request_id',
    'connection_id',
    'generation',
  ]),
  durable_ack: Object.freeze([
    'version',
    'type',
    'role_instance',
    'run_id',
    'market',
    'stream',
    'connection_id',
    'generation',
    'payload',
  ]),
  applied_ack: Object.freeze([
    'version',
    'type',
    'role_instance',
    'run_id',
    'market',
    'stream',
    'connection_id',
    'generation',
    'payload',
  ]),
  resend: Object.freeze([
    'version',
    'type',
    'role_instance',
    'request_id',
    'run_id',
    'market',
    'stream',
    'connection_id',
    'generation',
    'payload',
  ]),
  invalidate: Object.freeze([
    'version',
    'type',
    'role_instance',
    'request_id',
    'run_id',
    'connection_id',
    'generation',
    'payload',
  ]),
  invalidated: Object.freeze([
    'version',
    'type',
    'role_instance',
    'request_id',
    'run_id',
    'connection_id',
    'generation',
    'payload',
  ]),
  stop: Object.freeze(['version', 'type', 'role_instance', 'request_id']),
  tail_sealed: Object.freeze(['version', 'type', 'role_instance', 'run_id', 'payload']),
  drained: Object.freeze(['version', 'type', 'role_instance', 'run_id', 'payload']),
  stopped: Object.freeze(['version', 'type', 'role_instance', 'request_id']),
  error: Object.freeze(['version', 'type', 'role_instance', 'payload']),
  readiness: Object.freeze(['version', 'type', 'role_instance', 'payload']),
});

/** A non-empty string, the type every identity and id field has. */
function isNonEmptyString(value) {
  return typeof value === 'string' && value.length > 0;
}

/**
 * One validator per common field. A field is validated whenever it is present; a required field
 * that is absent, or present but of the wrong type, is refused. `payload` is deliberately
 * unconstrained: stage 1 defines the shell, not the bodies.
 */
const FIELD_VALIDATORS = Object.freeze({
  version: (value) => Number.isInteger(value) && value >= 1,
  type: (value) => typeof value === 'string' && TYPE_SET.has(value),
  request_id: isNonEmptyString,
  role_instance: isNonEmptyString,
  run_id: isNonEmptyString,
  market: isNonEmptyString,
  stream: isNonEmptyString,
  connection_id: isNonEmptyString,
  generation: (value) => Number.isInteger(value) && value >= 0,
  payload: () => true,
});

/** The required fields for a type, or null when the type is not in the vocabulary. */
export function requiredFieldsFor(type) {
  return REQUIRED_FIELDS_BY_TYPE[type] ?? null;
}

/** True only for a type the vocabulary defines. */
export function isKnownType(type) {
  return TYPE_SET.has(type);
}

/**
 * Validate one message and return it frozen in the common field order.
 *
 * Throws - never substitutes a default - when:
 *  - the value is not a plain object,
 *  - it names a field outside the common shape,
 *  - it names an unknown type,
 *  - a required field for its type is missing, or
 *  - any present field has the wrong type.
 */
export function makeMessage(fields) {
  if (fields === null || typeof fields !== 'object' || Array.isArray(fields)) {
    throw new TypeError('a message must be a plain object');
  }
  for (const key of Object.keys(fields)) {
    if (!COMMON_FIELDS.includes(key)) {
      throw new TypeError(`message carries a field outside the common shape: ${key}`);
    }
  }
  const { type } = fields;
  if (!isKnownType(type)) {
    throw new TypeError(`unknown message type ${JSON.stringify(type)}`);
  }
  const required = REQUIRED_FIELDS_BY_TYPE[type];
  const normalized = {};
  for (const field of COMMON_FIELDS) {
    const present = Object.prototype.hasOwnProperty.call(fields, field);
    if (!present) {
      if (required.includes(field)) {
        throw new TypeError(`message of type ${type} is missing required field ${field}`);
      }
      continue;
    }
    if (!FIELD_VALIDATORS[field](fields[field])) {
      throw new TypeError(
        `message field ${field} is invalid for type ${type}: ${JSON.stringify(fields[field])}`,
      );
    }
    normalized[field] = fields[field];
  }
  return Object.freeze(normalized);
}

/**
 * Serialize one control message for a TAG_CONTROL payload. Refuses an invalid message before it
 * reaches the wire, so a hole can never be sent.
 */
export function encodeMessage(message) {
  return Buffer.from(JSON.stringify(makeMessage(message)), 'utf8');
}

/**
 * Parse one control message off a TAG_CONTROL payload. The same contract is enforced here, so a
 * message that arrived missing a required field is refused rather than delivered with the hole
 * filled in by a default.
 */
export function decodeMessage(bytes) {
  if (!Buffer.isBuffer(bytes) && typeof bytes !== 'string') {
    throw new TypeError('a control payload must be a Buffer or a string');
  }
  const text = Buffer.isBuffer(bytes) ? bytes.toString('utf8') : bytes;
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new TypeError(`control payload is not JSON: ${error.message}`);
  }
  return makeMessage(parsed);
}
