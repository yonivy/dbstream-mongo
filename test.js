const { MongoMemoryServer } = require("mongodb-memory-server");
const mongodb = require("mongodb-legacy");
const assert = require("assert");
const test = require("dbstream/test");
const db = require("./mongo");


describe("DatabaseStream Mongo", function () {

    let mongod, mongoUri;
    const connect = mongodb.MongoClient.connect;

    before(async function () {
        mongodb.MongoClient.connect = connect;

        mongod = await MongoMemoryServer.create();
        mongoUri = mongod.getUri();
    })

    after(async function () {
        mongodb.MongoClient.connect = connect;

        await mongod.stop();
    })

    beforeEach(async function () {
        mongodb.MongoClient.connect = connect;
    })

    it("Implements the dbstream API", function (done) {
        const addr = mongoUri + "test1";
        const options = { collection: "test1" };
        const conn = db.connect(addr, options);

        const t = test(conn);

        t(done);
    });

    it("Supports multiple collections", async function () {
        const addr = mongoUri + "test2";

        const conn1 = new Connection(addr, { collection: "test2" });
        const conn2 = new Connection(addr, { collection: "test3" });

        await conn2.save({ hello: "world" });

        const res1 = await conn1.find({});
        const res2 = await conn2.find({});

        assert.equal(res1.length, 0);
        assert.equal(res2.length, 1);
        assert.equal(res2[0].hello, "world");
    })

    it("Throws connection errors", function (done) {
        mongodb.MongoClient.connect = function (url, options, callback) {
            callback({ err: "Something went wrong" });
        }

        const addr = mongoUri + "test4";
        const conn = db.connect(addr, { collection: "test4" });

        new conn.Cursor()
            .on("error", function (err) {
                assert(err instanceof Error);
                assert(err.message, "Something went wrong");
                done();
            })
            .end({ hello: "world" })
    })

    it("Retries connecting on timeout", function (done) {
        let called = 0;
        mongodb.MongoClient.connect = function (url, options, callback) {
            called += 1;
            return callback({ err: "connection to [127.0.0.1:27017] timed out" });
        }

        const addr = mongoUri + "test5";
        const conn = db.connect(addr, { collection: "test5", maxRetries: 3 });

        conn.on("error", function () {
            assert.equal(called, 3);
            done();
        })

        new conn.Cursor()
            .on("data", function () { })
            .on("error", done)
            .find({})
    })

    it("Creates a document", async function () {
        const addr = mongoUri + "test6";
        const conn = new Connection(addr);

        const value = 1;

        // no doc should exist before the insert
        const res1 = await conn.find({ value });
        assert.deepEqual(res1.length, 0);

        // now add the doc and verify we can find it
        await conn.save({ value });
        const res2 = await conn.find({ value });
        const doc = res2[0];

        assert.deepEqual(res2.length, 1);
        assert.deepEqual(doc.value, value);
        assert(doc.id);
    })

    it("Updates a document", async function () {
        const addr = mongoUri + "test7";
        const conn = new Connection(addr);

        const value1 = 1;
        const value2 = 2;

        // create the doc
        await conn.save({ value: value1 });
        const res1 = await conn.find({ value: value1 });
        const doc1 = res1[0];

        assert.deepEqual(res1.length, 1);
        assert.deepEqual(doc1.value, value1);

        // update the doc
        await conn.save({ value: value2, id: doc1.id });
        const res2 = await conn.find({ value: value2 });
        const doc2 = res2[0];

        assert.deepEqual(res2.length, 1);
        assert.deepEqual(doc2.value, value2);

        // verify we worked on the same doc
        assert.deepEqual(doc1.id, doc2.id);
    })

    it("Deletes a document", async function () {
        const addr = mongoUri + "test8";
        const conn = new Connection(addr);

        const value = 1;

        // add the doc and verify we can find it
        await conn.save({ value });
        const res1 = await conn.find({ value });
        assert.deepEqual(res1.length, 1);

        // now delete the doc and verify we can't find it
        await conn.drop({ id: res1[0].id });
        const res2 = await conn.find({ id: res1[0].id });
        assert.deepEqual(res2.length, 0);
    })

    it("Finds many documents", async function () {
        const addr = mongoUri + "test9";
        const conn = new Connection(addr);

        const value = 'a';

        // make sure we start with a clean plate
        const res1 = await conn.find({});
        assert.deepEqual(res1.length, 0);

        // now add the doc and verify we can find it
        await conn.save({ value });
        await conn.save({ value });
        await conn.save({ value: 'b' });
        const res2 = await conn.find({ value });

        assert.equal(res2.length, 2);
        assert.equal(res2[0].value, value);
        assert.equal(res2[1].value, value);
        assert.notEqual(res2[0].id, res2[1].id);
    })
});

/**
 * This promise based API aims to reduce the boilerplate of properly handling
 * different stream events and function calls.
 *
 * An API like this can be useful in the package itself but making it production
 * ready will require more thought and effort than there's time and need to right now.
 */
class Connection {
    constructor(addr, options = { collection: 'test' }) {
        this._conn = db.connect(addr, options)
    }

    async find(query) {
        const results = []

        return new Promise((resolve, reject) => {
            new this._conn.Cursor()
                .on("error", reject)
                .on("end", () => resolve(results))
                .on("data", results.push.bind(results))
                .find(query)
        })
    }

    async save(object) {
        return new Promise((resolve, reject) => {
            new this._conn.Cursor()
                .on("error", reject)
                .on("finish", resolve)
                .end(object)
        })
    }

    async drop(object) {
        return new Promise((resolve, reject) => {
            const cursor = new this._conn.Cursor()

            cursor.on("error", reject)
                .on("finish", resolve)
                .remove(object)

            cursor.end()
        })
    }
}
