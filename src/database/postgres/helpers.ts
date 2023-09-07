import pgTypes = require('pg-types');

// Used ChatGPT to generate the definition of the helpers object
const helpers: {
    valueToString: (value: number) => string;
    removeDuplicateValues: (values: string[], ...others:string[][]) => void;
    ensureLegacyObjectType: (db, key: string, type:string) => Promise<void>;
    ensureLegacyObjectsType: (db, keys: string[], type: string) => Promise<void>;
} = {
    valueToString: function (): string {
        throw new Error('Function not implemented.');
    },
    removeDuplicateValues: function (): void {
        throw new Error('Function not implemented.');
    },
    ensureLegacyObjectType: function (): Promise<void> {
        throw new Error('Function not implemented.');
    },
    ensureLegacyObjectsType: function (): Promise<void> {
        throw new Error('Function not implemented.');
    },
};

module.exports = helpers;

interface QueryConfig<I extends any[] = any[]> {
    name?: string | undefined;
    text: string;
    values?: I | undefined;
    types?: CustomTypesConfig | undefined;
}

interface CustomTypesConfig {
    getTypeParser: typeof pgTypes.getTypeParser;
}

interface QueryArrayConfig<I extends any[] = any[]> extends QueryConfig<I> {
    rowMode: 'array';
}

interface FieldDef {
    name: string;
    tableID: number;
    columnID: number;
    dataTypeID: number;
    dataTypeSize: number;
    dataTypeModifier: number;
    format: string;
}

interface QueryResultBase {
    command: string;
    rowCount: number;
    oid: number;
    fields: FieldDef[];
}

interface QueryResultRow {
    [column: string]: any;
}

interface QueryResult<R extends QueryResultRow = any> extends QueryResultBase {
    rows: R[];
}

interface QueryArrayResult<R extends any[] = any[]> extends QueryResultBase {
    rows: R[];
}

helpers.valueToString = function (value: number) : string {
    return String(value);
};

helpers.removeDuplicateValues = function (values: string[], ...others:string[][]) {
    for (let i = 0; i < values.length; i++) {
        if (values.lastIndexOf(values[i]) !== i) {
            values.splice(i, 1);
            for (let j = 0; j < others.length; j++) {
                others[j].splice(i, 1);
            }
            i -= 1;
        }
    }
};


helpers.ensureLegacyObjectType = async function (db, key: string, type:string) {
    // The next line calls a function in a module that has not been updated to TS yet (db.query)
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    await db.query({
        name: 'ensureLegacyObjectTypeBefore',
        text: `
DELETE FROM "legacy_object"
 WHERE "expireAt" IS NOT NULL
   AND "expireAt" <= CURRENT_TIMESTAMP`,
    });

    // The next line calls a function in a module that has not been updated to TS yet (db.query)
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    await db.query({
        name: 'ensureLegacyObjectType1',
        text: `
INSERT INTO "legacy_object" ("_key", "type")
VALUES ($1::TEXT, $2::TEXT::LEGACY_OBJECT_TYPE)
    ON CONFLICT
    DO NOTHING`,
        values: [key, type],
    });

    // The next line calls a function in a module that has not been updated to TS yet (db.query)
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    const res: QueryResult<any> = await db.query({
        name: 'ensureLegacyObjectType2',
        text: `
SELECT "type"
  FROM "legacy_object_live"
 WHERE "_key" = $1::TEXT`,
        values: [key],
    }) as QueryResult<any>;

    // The next line calls a field rows in a module that has not been updated to TS yet
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    if (res.rows[0].type !== type) {
        // The next line calls a field rows in a module that has not been updated to TS yet
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        throw new Error(`database: cannot insert ${JSON.stringify(key)} as ${type} because it already exists as ${String(res.rows[0].type)}`);
    }
};

helpers.ensureLegacyObjectsType = async function (db, keys: string[], type: string) {
    // The next line calls a function in a module that has not been updated to TS yet (db.query)
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    await db.query({
        name: 'ensureLegacyObjectTypeBefore',
        text: `
DELETE FROM "legacy_object"
 WHERE "expireAt" IS NOT NULL
   AND "expireAt" <= CURRENT_TIMESTAMP`,
    });

    // The next line calls a function in a module that has not been updated to TS yet (db.query)
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    await db.query({
        name: 'ensureLegacyObjectsType1',
        text: `
INSERT INTO "legacy_object" ("_key", "type")
SELECT k, $2::TEXT::LEGACY_OBJECT_TYPE
  FROM UNNEST($1::TEXT[]) k
    ON CONFLICT
    DO NOTHING`,
        values: [keys, type],
    });

    // The next line calls a function in a module that has not been updated to TS yet (db.query)
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    const res: QueryResult<any> = await db.query({
        name: 'ensureLegacyObjectsType2',
        text: `
SELECT "_key", "type"
  FROM "legacy_object_live"
 WHERE "_key" = ANY($1::TEXT[])`,
        values: [keys],
    }) as QueryResult<any>;

    const invalid = res.rows.filter((r: { type: string; }) => r.type !== type);

    if (invalid.length) {
        const parts = invalid.map((r: { _key: string; type: string; }) => `${JSON.stringify(r._key)} is ${r.type}`);
        throw new Error(`database: cannot insert multiple objects as ${type} because they already exist: ${parts.join(', ')}`);
    }

    const missing = keys.filter(k => !res.rows.some((r: { _key: string; }) => r._key === k));

    if (missing.length) {
        throw new Error(`database: failed to insert keys for objects: ${JSON.stringify(missing)}`);
    }
};

