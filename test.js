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

    it("Implements the dbstream API", function (done) {
        const addr = mongoUri + "test1";
        const options = { collection: "test1" }
        const conn = db.connect(addr, options)

        const t = test(conn);

        t(done);
    });

    it("Supports multiple collections", function (done) {
        const addr = mongoUri + "test2";
        const options1 = { collection: "test2" }
        const options2 = { collection: "test3" }
        const conn1 = db.connect(addr, options1)
        const conn2 = db.connect(addr, options2)

        const data = [];
        new conn1.Cursor()
            .on("error", done)
            .on("finish", function () {
                new conn2.Cursor()
                    .on("error", done)
                    .on("data", data.push.bind(data))
                    .on("end", function () {
                        assert.deepEqual(data, []);
                        done();
                    })
                    .find({})
            })
            .end({ hello: "world" })
    })

    it("Throws connection errors", function (done) {
        mongodb.MongoClient.connect = function (url, options, callback) {
            callback({ err: "Something went wrong" });
        }

        const addr = mongoUri + "test4";
        const conn = db.connect(addr, { collection: "test4" })

        new conn.Cursor()
            .on("error", function (err) {
                assert(err.message, "Something went wrong")
                done();
            })
            .end({ hello: "world" })
    })

    it("Retries connecting on timeout", function (done) {
        let called = 0
        mongodb.MongoClient.connect = function (url, options, callback) {
            called += 1
            return callback({ err: "connection to [127.0.0.1:27017] timed out" });
        }

        const addr = mongoUri + "test5";
        const conn = db.connect(addr, { collection: "test5", maxRetries: 3 })

        conn.on("error", function () {
            assert.equal(called, 3)
            done()
        })

        new conn.Cursor()
            .on("data", function () { })
            .on("error", done)
            .find({})
    })

});
