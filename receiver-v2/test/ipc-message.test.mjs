import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  COMMON_FIELDS,
  IPC_VERSION,
  MESSAGE_TYPES,
  REQUIRED_FIELDS_BY_TYPE,
  decodeMessage,
  encodeMessage,
  isKnownType,
  makeMessage,
  requiredFieldsFor,
} from '../src/ipc-message.mjs';

/** A base with every common field set to a valid value; only the required ones are kept. */
function validFields(type, overrides = {}) {
  const base = {
    version: IPC_VERSION,
    type,
    request_id: 'req-1',
    role_instance: 'organize-1',
    run_id: 'run-1',
    market: 'kraken_spot',
    stream: 'trades',
    connection_id: 'conn-1',
    generation: 0,
    payload: { body: true },
  };
  const fields = {};
  for (const field of REQUIRED_FIELDS_BY_TYPE[type]) fields[field] = base[field];
  return { ...fields, ...overrides };
}

test('the vocabulary and the required-fields table describe exactly the same types', () => {
  assert.deepEqual(Object.keys(REQUIRED_FIELDS_BY_TYPE), MESSAGE_TYPES);
  for (const type of MESSAGE_TYPES) {
    const required = requiredFieldsFor(type);
    assert.ok(required.includes('version'), `${type} must carry version`);
    assert.ok(required.includes('type'), `${type} must carry type`);
    assert.ok(required.includes('role_instance'), `${type} must carry role_instance`);
    for (const field of required) {
      assert.ok(COMMON_FIELDS.includes(field), `${type}'s required field ${field} is not in the shape`);
    }
    assert.ok(Object.isFrozen(required), `${type}'s required list must be frozen`);
  }
  assert.equal(isKnownType('durable_ack'), true);
  assert.equal(isKnownType('ack'), false, 'the old bare ack is not in the vocabulary');
});

test('every type round-trips through encode and decode with its required fields', () => {
  for (const type of MESSAGE_TYPES) {
    const fields = validFields(type);
    const message = makeMessage(fields);
    const decoded = decodeMessage(encodeMessage(fields));
    assert.deepEqual(decoded, message, `${type} did not survive the round trip`);
    assert.ok(Object.isFrozen(decoded));
  }
});

test('a message missing any one required field is refused at make, encode and decode', () => {
  for (const type of MESSAGE_TYPES) {
    for (const field of REQUIRED_FIELDS_BY_TYPE[type]) {
      const fields = validFields(type);
      delete fields[field];
      // An absent `type` cannot name a table, so it is refused as an unknown type; every other
      // absent required field is refused by name.
      const pattern = field === 'type' ? /unknown message type/ : /missing required field/;
      assert.throws(() => makeMessage(fields), pattern, `${type} without ${field}`);
      assert.throws(() => encodeMessage(fields), pattern, `${type} encode without ${field}`);
      assert.throws(
        () => decodeMessage(Buffer.from(JSON.stringify(fields), 'utf8')),
        pattern,
        `${type} decode without ${field}`,
      );
    }
  }
});

test('an unknown type is refused on both ends, never treated as a message', () => {
  const fields = { version: IPC_VERSION, type: 'ack', role_instance: 'organize-1' };
  assert.throws(() => makeMessage(fields), /unknown message type/);
  assert.throws(() => encodeMessage(fields), /unknown message type/);
  assert.throws(() => decodeMessage(Buffer.from(JSON.stringify(fields), 'utf8')), /unknown message type/);
});

test('a field outside the common shape is refused, not silently dropped', () => {
  const fields = { ...validFields('stop'), extra: 1 };
  assert.throws(() => makeMessage(fields), /outside the common shape/);
  assert.throws(() => decodeMessage(Buffer.from(JSON.stringify(fields), 'utf8')), /outside the common shape/);
});

test('a present field of the wrong type is refused', () => {
  const cases = [
    { version: '1' },
    { version: 0 },
    { role_instance: '' },
    { role_instance: 7 },
    { connection_id: '' },
    { generation: -1 },
    { generation: '0' },
  ];
  for (const overrides of cases) {
    assert.throws(() => makeMessage(validFields('durable_ack', overrides)), /invalid for type/);
  }
});

test('payload is carried untouched and is never interpreted by the shell', () => {
  const shapes = [null, 42, 'text', [1, 2, 3], { deep: { nested: ['x'] } }];
  for (const payload of shapes) {
    const message = makeMessage(validFields('durable_ack', { payload }));
    const decoded = decodeMessage(encodeMessage(validFields('durable_ack', { payload })));
    assert.deepEqual(decoded.payload, payload);
    assert.equal(message.payload, payload);
  }
});

test('only a plain object is a message: arrays and scalars are refused', () => {
  for (const value of [null, 42, 'hello', [1, 2]]) {
    assert.throws(() => makeMessage(value), /must be a plain object/);
  }
  assert.throws(() => decodeMessage(Buffer.from('[1,2]', 'utf8')), /must be a plain object/);
});

test('a control payload that is not JSON is refused', () => {
  assert.throws(() => decodeMessage(Buffer.from('not json', 'utf8')), /not JSON/);
  assert.throws(() => decodeMessage(123), /Buffer or a string/);
});

test('an optional field, when supplied, is validated too', () => {
  // stop does not require request_id, but if one is there it must be a non-empty string.
  assert.doesNotThrow(() => makeMessage(validFields('stop', { request_id: 'req-2' })));
  assert.throws(() => makeMessage(validFields('stop', { request_id: '' })), /invalid for type/);
});
