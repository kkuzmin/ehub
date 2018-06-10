/* -----------------------------------------------------------------------------
 * @copyright (C) 2017, Alert Logic, Inc
 * @doc
 *
 * The purpose of this function it to be registered as an O365 webhook and
 * receive/process notifications.
 * https://msdn.microsoft.com/en-us/office-365/office-365-management-activity-api-reference#receiving-notifications
 *
 * @end
 * -----------------------------------------------------------------------------
 */

const async = require('async');
const zlib = require('zlib');

const m_o365mgmnt = require('../lib/o365_mgmnt');
const m_ingestProto = require('./ingest_proto');
const m_ingest = require('./ingest');

const g_ingestc = new m_ingest.Ingest(
        process.env.APP_INGEST_ENDPOINT,
        {
            access_key_id : process.env.CUSTOMCONNSTR_APP_CI_ACCESS_KEY_ID,
            secret_key: process.env.CUSTOMCONNSTR_APP_CI_SECRET_KEY
        }
);

// One O365 content message is about 1KB.
var MAX_BATCH_MESSAGES = 1500;

module.exports ={
    processContent : processContent
};

var processContent = function (context, events, callback) {
    return parseContent(context, events,
        function(err, parsedContent) {
            if (err) {
                return asyncCallback(err);
            }
            else {
                return sendToIngest(context, parsedContent, asyncCallback);
            }
    });
};

function getEventTs(context, event) {
    var eventTs = 
        event.eventTimestamp ? event.eventTimestamp :
        event.time ? event.time :
        event.CreationTime ? event.CreationTime :
        undefined;
    
    
    if (eventTs == undefined) {
        context.log.warn('Unable to parse CreationTime from content.');
        return Math.floor(Date.now() / 1000);
    }
    else {
        return Math.floor(Date.parse(eventTs) / 1000);
    }
}

function getEventType(event) {
    return  event.operationName ? event.operationName :
        event.RecordType ? event.RecordType.toString() :
        "undefined";
}

// Parse each message into:
// {
//  hostname: <smth>
//  message_ts: <CreationTime from the message>
//  message: <string representation of msg>
// }
function parseContent(context, events, callback) {
    async.reduce(events, [], function(memo, item, callback) {
            var message;
            try {
                message = JSON.stringify(item);
            }
            catch(err) {
                return callback(`Unable to stringify content. ${err}`);
            }

            var newItem = {
                message_ts: getEventTs(context, item),
                record_type: getEventType(item),
                message: message
            };

            memo.push(newItem);
            return callback(null, memo);
        },
        function(err, result) {
            if (err) {
                return callback(`Content parsing failure. ${err}`);
            } else {
                return callback(null, result);
            }
        }
    );
}

function sendToIngest(context, content, callback) {
    async.waterfall([
        function(asyncCallback) {
            m_ingestProto.load(context, function(err, root) {
                asyncCallback(err, root);
            });
        },
        function(root, asyncCallback) {
            m_ingestProto.setMessage(context, root, content, function(err, msg) {
                asyncCallback(err, root, msg);
            });
        },
        function(root, msg, asyncCallback) {
            m_ingestProto.setHostMetadata(context, root, content, function(err, meta) {
                asyncCallback(err, root, meta, msg);
            });
        },
        function(root, meta, msg, asyncCallback) {
            m_ingestProto.setBatch(context, root, meta, msg, function(err, batch) {
                asyncCallback(err, root, batch);
            });
        },
        function(root, batchBuf, asyncCallback) {
            m_ingestProto.setBatchList(context, root, batchBuf,
                function(err, batchList) {
                    asyncCallback(err, root, batchList);
                });
        },
        function(root, batchList, asyncCallback) {
            m_ingestProto.encode(context, root, batchList, asyncCallback);
        }],
        function(err, result) {
            if (err) {
                return callback(err);
            }

            zlib.deflate(result, function(err, compressed) {
                if (err) {
                    return callback(`Unable to compress. ${err}`);
                } else {
                    if (compressed.byteLength > 700000)
                        context.log.warn(`Compressed log batch length`,
                            `(${compressed.byteLength}) exceeds maximum allowed value.`);
                    return g_ingestc.sendO365Data(compressed)
                        .then(resp => {
                            context.log.verbose('Bytes sent to Ingest: ', compressed.byteLength);
                            return callback(null, resp);
                        })
                        .catch(function(exception){
                            return callback(`Unable to send to Ingest. ${exception}`);
                        });
                }
            });
        });
}
