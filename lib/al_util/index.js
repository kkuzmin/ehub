/* -----------------------------------------------------------------------------
 * @copyright (C) 2017, Alert Logic, Inc
 * @doc
 * 
 * Helper utilities for  Alertlogic log collector.
 * 
 * @end
 * -----------------------------------------------------------------------------
 */
 
const rp = require('request-promise-native');
const path = require('path');

let MAX_CONNS_PER_SERVICE = 128;

/**
 * @class
 * Initializes a new instance of the AzureServiceClient class.
 *
 * @constructor
 * @param {string} endpoint - hostname/address to sent HTTPS
 * requests to. 
 * 
 */
class RestServiceClient {
    constructor(endpoint) {
        this._host = endpoint;
        this._url = 'https://' + endpoint;
        this._pool = {
            maxSockets: MAX_CONNS_PER_SERVICE
        };
    }
    _initRequestOptions(method, path, extra) {
        const defaultOptions = {
            method: method,
            url: this._url + path,
            json: true,
            headers: {},
            pool: this._pool
        };
        const options = Object.assign({}, defaultOptions, extra);
        const defaultHeaders = {
            'Accept': 'application/json'
        };
        Object.assign(options.headers, 
                      defaultHeaders, 
                      extra.headers ? extra.headers : {});
        return options;
    }
    request(method, path, extraOptions) {
        const options = this._initRequestOptions(method, path, extraOptions);
        return rp(options);
    }
    post(path, extraOptions) {
        return this.request('POST', path, extraOptions);
    }
    get(path, extraOptions) {
        return this.request('GET', path, extraOptions);
    }
    get host() {
        return this._host;
    }
    get url() {
        return this._url;
    }
}

exports.RestServiceClient = RestServiceClient;

exports.getADCacheFilename = function(options) {
    return path.join(process.env.TMP,
        encodeURIComponent(options.creds.client_id) + '-' +
        encodeURIComponent(options.creds.tenant_id) + '-' +
        encodeURIComponent(options.resource) + '-token.tmp');
}
