function stableStringify(value: unknown, ancestors: Set<object>): string {
  if (value === null || typeof value !== 'object') {
    const serialized = JSON.stringify(value);
    if (serialized === undefined) {
      throw new TypeError('Task payload must be JSON-serializable');
    }
    return serialized;
  }

  if (ancestors.has(value)) {
    throw new TypeError('Task payload must not contain circular references');
  }

  ancestors.add(value);
  try {
    const toJSON = (value as { toJSON?: () => unknown }).toJSON;
    if (typeof toJSON === 'function') {
      return stableStringify(toJSON.call(value), ancestors);
    }

    if (Array.isArray(value)) {
      return `[${value
        .map((item) =>
          item === undefined ? 'null' : stableStringify(item, ancestors),
        )
        .join(',')}]`;
    }

    const entries = Object.keys(value as Record<string, unknown>)
      .sort()
      .flatMap((key) => {
        const item = (value as Record<string, unknown>)[key];
        return item === undefined
          ? []
          : [`${JSON.stringify(key)}:${stableStringify(item, ancestors)}`];
      });
    return `{${entries.join(',')}}`;
  } finally {
    ancestors.delete(value);
  }
}

export function createTaskFingerprint(type: string, payload: unknown): string {
  return `${JSON.stringify(type)}:${stableStringify(payload, new Set())}`;
}
