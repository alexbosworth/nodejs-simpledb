// Use: init: var sdb = require('aws-sdb').init(TOKEN, SECRET);
// sdb().get(domain, key);
// sdb().put(domain, key, value);
// 
// sdb().get(domain, key).success(function(row) { }).
//                        failure(function(err) { }).
//                        complete(function(err, response, [data]));

var crypto = require('crypto'),
    http = require('http'),
    
    expat = require('./lib/node-expat/build/default/node-expat'),
    querystring = require('./lib/Node-Querystring/querystring');

function init(key, pass, options) {
    options = options || {};
    
    return function createInstance(newOptions) {
        return new Sdb(key, pass, newOptions || options);
    };
}

function Sdb(key, pass, domain, options) {
    this._key = key;
    this._pass = pass;
    
    this._params = {
        AWSAccessKeyId: this._key,
        Version: '2009-04-15',
        SignatureVersion: '2',
        SignatureMethod: 'HmacSHA256'
    };
	
	this._retries = 0;
	
	this._host = 'sdb.amazonaws.com';
        
    this._successCbk = new Function();
    this._failureCbk = new Function();
    this._completeCbk = new Function();
    
    return this;
}

Sdb.prototype.success = function(cbk) {
    this._successCbk = cbk;
    
    return this;
};

Sdb.prototype.failure = function(cbk) {
    this._failureCbk = cbk;
    
    return this;
};

Sdb.prototype.complete = function(cbk) {
    this._completeCbk = cbk;
    
    return this;
};

Sdb.prototype._getSignature = function() { 
    var self = this,
        query = self._params,
        keys = [],
        sorted = {};

    for (var key in query) keys.push(key);

    keys = keys.sort();

    for (var k in keys) {
        var key = keys[k];
        sorted[key] = query[key];
    }

    var stringToSign = ["POST", self._host, '/', 
        querystring.stringify(sorted)].join("\n");
    
    stringToSign = stringToSign.replace(/'/gm, "%27");
    stringToSign = stringToSign.replace(/\*/gm, "%2A");
    stringToSign = stringToSign.replace(/\(/gm, "%28");
    stringToSign = stringToSign.replace(/\)/gm, "%29");
    stringToSign = stringToSign.replace(/!/gm, '%21');
    
    var hash = crypto.createHmac("sha256", this._pass);

    return hash.update(stringToSign.toString()).digest("base64");
};

Sdb.prototype.request = function(action) { 
    var self = this;
    
	if (!action && !self._params.Action) throw new Error('no type of request specified');
		
    self._params.Action = self._params.Action || action;
	self._params.Timestamp = new Date(Math.round(new Date().getTime() /1000) *1000).toISOString();
	self._params.Signature = self._getSignature();
	
	var body = querystring.stringify(self._params);
	
    var options = {
        host: self._host,
        port: 80,
        path: '/',
        method: 'POST',
        headers: {
            'Content-Length': body.length,
    		'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
         	'Host': self._host
        }
    };
    
    var request = http.request(options, function(r) { self._processResponse(r); }).
    on('error', function(err) { console.log(err); });
    
    request.write(body); 
    
    request.end();
    	
	if (!self._requestStart) self._requestStart = new Date().getTime();
		
	return self;
};

Sdb.prototype._processResponse = function(response) { 
	var self = this,
	    data = '';
	
	response.on('data', function(chunk) { data += chunk; });
	
	response.on('end', function() {
	    var parser = new expat.Parser("UTF-8");
	    
	    var results = [],
	        resultsTag = self._params.Action + 'Result',
	        responseTag = self._params.Action + 'Response',
	        actionSuccess = self[self._params.Action + 'Success'],
	        isResult = false,
	        tagName = null;
	    
        parser.on('startElement', function(name, attrs) {
            if (name == resultsTag) return isResult = true;            
        });
        
        parser.on('endElement', function(name) {
            if (name == resultsTag) return isResult = false;
            
            if (name == responseTag) return actionSuccess(results);
        })
        
        parser.on('text', function(str) { 
            if (isResult) return results.push(str);
        });
        
        parser.parse(data);
	});
};

Sdb.prototype.listDomains = function() {
	return this.request('ListDomains');
};

Sdb.prototype.ListDomainsSuccess = function(results) { 
    console.log(results);
};

exports.init = init;