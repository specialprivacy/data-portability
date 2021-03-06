const express = require("express");
const app = express();

const http = require("http");
http.globalAgent.maxSockets = 10;

const uuidv4 = require('uuid/v4');

const requests = {};
const responses = {};
const dataSubjects = {};

app.disable("x-powered-by");

app.use(express.json({type: "application/vnd.api+json"}));
app.use(express.urlencoded({ extended: true }));

// TODO: check if still needed
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, Content-Length, X-Requested-With, APP_KEY");

    // intercepts OPTIONS method
    if (req.method === "OPTIONS") {
        // respond with 200
        res.status(200).send()
    }
    next()
});

app.use(function (req, res, next) {
    res.type("application/vnd.api+json");
    next();
});

app.get("/data-portability-requests", (req, res) => {
    let payload = {
        "data": Object.values(requests).map((request) => { return request.toJsonApi() })
    };
    if(req.query.include && req.query.include.includes("data-portability-response")) {
        payload["included"] = [].concat.apply(
            Object.values(requests)
            .filter((request) => {return request.dataPortabilityResponseId != null})
            .map((request) => {
                const response = responses[request.dataPortabilityResponseId];
                return [response.toJsonApi(), ...response.getIncluded()];
            }));
    }

    res.status(200).json(payload);
});

app.post("/data-portability-requests", (req, res) => {
    const uuid = uuidv4();
    requests[uuid] = new DataPortabilityRequest(uuid, new Date(), req.body.data.relationships["data-subject"].data.id, null);

    let payload = {"data": requests[uuid].toJsonApi()};

    try {
        console.debug({topic: dataPortabilityRequestsTopic, payload: JSON.stringify(payload)}, "Producing on topic");
        producer.produce(
            dataPortabilityRequestsTopic, // Topic
            null, // Partition, null uses default
            Buffer.from(JSON.stringify(payload)), // Message
            null,
            Date.now()
        )
    } catch (error) {
        log.error({topic: dataPortabilityRequestsTopic, err: error}, "An error occurred when trying to send message to Kafka topic")
    }


    res.status(201).json(payload);
});

app.get("/data-portability-requests/:id", (req, res) => {
    let uuid = req.params.id;
    let request = requests[uuid];
    if(!request) return res.status(404).end();

    let payload = {"data": request.toJsonApi()};
    if(req.query.include && req.query.include.includes("data-portability-response")) {
        if(request.dataPortabilityResponseId) {
            const response = responses[request.dataPortabilityResponseId].toJsonApi();
            payload["included"] = [response.toJsonApi(), ...response.getIncluded()];
        }
    }

    res.status(200).json(payload);
});

app.delete("/data-portability-requests/:id", (req, res) => {
    let uuid = req.params.id;
    if(requests[uuid].dataPortabilityResponseId) {
        delete responses[requests[uuid].dataPortabilityResponseId];
    }
    delete requests[uuid];

    res.status(204).end();
});


app.get("/data-portability-responses", (req, res) => {
    let payload = {
        "data": Object.values(responses).map((response) => { return response.toJsonApi() }),
        "included": [].concat.apply([],
            Object.values(responses).map((response) => { return response.getIncluded() })
        )
    };
    if(req.query.include && req.query.include.includes("data-portability-request")) {
        payload["included"] = payload["included"].concat(Object.values(responses)
            .filter((response) => {return response.dataPortabilityRequestId != null})
            .map((response) => { return requests[response.dataPortabilityRequestId].toJsonApi()}));
    }

    res.status(200).json(payload);
});

app.post("/data-portability-responses", (req, res) => {
    const uuid = uuidv4();

    const request = requests[req.body.data.relationships["data-portability-request"].data.id];
    if(!request) {
        return res.status(404).end();
    }

    responses[uuid] = new DataPortabilityResponse(uuid, new Date(), req.body.data.attributes.payload, request.id);
    request.dataPortabilityResponseId = uuid;

    let payload = {"data": responses[uuid].toJsonApi()};
    res.status(201).json(payload);
});

app.get("/data-portability-responses/:id", (req, res) => {
    let uuid = req.params.id;
    let response = responses[uuid];
    if(!response) return res.status(404).end();

    let payload = {"data": response.toJsonApi(), "included": response.getIncluded()};
    if(req.query.include && req.query.include.includes("data-portability-request")) {
        if(response.dataPortabilityRequestId) {
            payload["included"] = payload["included"].concat([requests[response.dataPortabilityRequestId].toJsonApi()]);
        }
    }

    res.status(200).json(payload);
});

app.delete("/data-portability-responses/:id", (req, res) => {
    let uuid = req.params.id;
    if(!responses[uuid]) {
        return res.status(404).end();
    }

    for (let request of Object.values(requests)) {
        if(request.dataPortabilityResponseId === uuid) {
            request.dataPortabilityResponseId = null;
            break;
        }
    }
    delete responses[uuid];

    res.status(204).end();
});

app.get("/data-subjects", (req, res) => {
    let payload = {"data": Object.values(dataSubjects).map((dataSubject) => { return dataSubject.toJsonApi() })};
    res.status(200).json(payload);
});

app.post("/data-subjects", (req, res) => {
    const uuid = req.body.data.id || uuidv4();
    dataSubjects[uuid] = new DataSubject(uuid, req.body.data.attributes["name"]);
    let payload = {"data": dataSubjects[uuid].toJsonApi()};
    res.status(201).json(payload);
});

app.get("/data-subjects/:id", (req, res) => {
    let uuid = req.params.id;
    let dataSubject = dataSubjects[uuid];
    if(!dataSubject) return res.status(404).end();
    let payload = {"data": dataSubject.toJsonApi()};
    res.status(200).json(payload);
});

class DataPortabilityRequest {
    constructor(id, timestamp, dataSubjectId, dataPortabilityResponseId) {
        this.id = id;
        this.timestamp = timestamp;
        this.dataSubjectId = dataSubjectId;
        this.dataPortabilityResponseId = dataPortabilityResponseId;
    }

    toJsonApi() {
        let payload = {
            "id": this.id,
            "type": "data-portability-requests",
            "attributes": {
                "timestamp": this.timestamp
            },
            "relationships": {}
        };
        if(this.dataSubjectId) {
            payload.relationships["data-subject"] = {
                "data": {
                    "id": this.dataSubjectId,
                    "type": "data-subjects"
                }
            }
        }
        if(this.dataPortabilityResponseId) {
            payload.relationships["data-portability-response"] = {
                "data": {
                    "id": this.dataPortabilityResponseId,
                    "type": "data-portability-responses"
                }
            }
        }
        return payload;
    }
}

class DataPortabilityResponse {
    constructor(id, timestamp, payload, dataPortabilityRequestId) {
        this.id = id;
        this.timestamp = timestamp;
        this.dataPortabilityRequestId = dataPortabilityRequestId;
        this.convertedPayload = this.convertPayload(payload);
    }

    toJsonApi() {
        return this.convertedPayload.data;
    }

    getIncluded() {
        return this.convertedPayload.included || [];
    }

    convertPayload(payload) {
        if(!payload["data"]) payload["data"] = {"attributes": {}, "relationships": {}, "links": {}};
        payload["data"]["id"] = this.id;
        payload["data"]["type"] = "data-portability-responses";
        payload["data"]["attributes"]["timestamp"] = this.timestamp;
        payload["data"]["links"]["self"] = "/data-portability-responses/" + this.id;
        payload["data"]["relationships"]["data-portability-request"] = {
            "data": {
                "id": this.dataPortabilityRequestId,
                "type": "data-portability-requests"
            }
        };
        delete payload["data"]["attributes"]["is-initialized"];
        delete payload["data"]["attributes"]["title"];
        return payload;
    }
}

class DataSubject {
    constructor(id, name) {
        this.id = id;
        this.name = name;
    }

    toJsonApi() {
        return {
            "id": this.id,
            "type": "data-subjects",
            "attributes": {
                "name": this.name
            },
            "relationships": {}
        };
    }
}

const Kafka = require("node-rdkafka");
const producer = new Kafka.Producer({
    "metadata.broker.list": process.env["KAFKA_BROKER_LIST"] || "localhost:9092",
    "api.version.request": false
});

const dataPortabilityRequestsTopic = process.env["DATA_PORTABILITY_REQUESTS_TOPIC"] || "data-portability-requests";

const MAX_RETRIES = 10;
let retryCount = 0;
let server;

function setup () {
    const connectOptions = {"timeout": process.env["KAFKA_TIMEOUT"] || 5000};
    producer.connect(connectOptions);
    producer.on("connection.failure", function (error) {
        if (retryCount >= MAX_RETRIES) {
            console.error({err: error}, "Could not connect to Kafka, exiting");
            process.exit(1);
        }
        retryCount++;
        const timeout = (Math.pow(2, retryCount) + Math.random()) * 1000;
        console.warn({err: error, timeout, retryCount}, `Failed to connect to kafka, retrying in ${timeout} ms`);
        setTimeout(producer.connect.bind(producer), timeout, connectOptions)
    });
    producer.on("event.error", function (error) {
        console.error({err: error}, "Error from kafka producer")
    });
    producer.on("delivery-report", function (error, report) {
        if (error) {
            console.error({err: error}, "Error in kafka delivery report")
        } else {
            console.info({report}, "Kafka delivery report")
        }
    });
    producer.setPollInterval(100);

    producer.on("ready", async () => {
        console.debug("Kafka producer ready.");
        server = app.listen(8080, () => {
            const { address } = server.address();
            const { port } = server.address();
            console.debug("App listening at http://%s:%s", address, port);
        });
    })
}

setup();



// Handle SIGTERM gracefully
process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);
process.on("SIGHUP", gracefulShutdown);
function gracefulShutdown () {
    // Serve existing requests, but refuse new ones
    console.warn("Received signal to terminate: wrapping up existing requests");
    server.close(() => {
        // Exit once all existing requests have been served
        console.warn("Received signal to terminate: done serving existing requests. Exiting");
        process.exit(0)
    })
}

