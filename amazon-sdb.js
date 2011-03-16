/*
 * 	Alex Bosworth 
 * 
 * 	A straightforward SimpleDb library
 * 
 *  USE: var sdb = new S3(AWS_KEY, AWS_SECRET);
 *       sdb.putAttributes(DOMAIN, KEY, DATA);
 *       sdb.getAttributes(DOMAIN, KEY).on('success', function(data) { });
 *       (more operations: createDomain, select, deleteDomain, etc)
 * 
 *  You can also use put() and get() as shorthand notation
 *  
 *  EVENTS: on('complete'): returns a raw xml response dump
 *          on('success') : data formatted nicely
 *          on('failure') : problem?
 *
 *  Tips:
 *  putAttributes - put multiple values == {key:[val,val2]} 
 *                  put multiple values + overwrite == {key:[[val,val2]]}
 *
 *  failure and success emit data, responseMetadata (includes: time, boxUsage, etc)
 * 
 *  REQUIRES: xml2js, http://bit.ly/node-jquery
 */
 
require.paths.unshift(__dirname);

var crypto = require('crypto'), 
	http = require('http'),		 
	EventEmitter = require('events').EventEmitter,
	$ = require('node-jquery'),
	xml2js = require('vendor/xml2js'),
	querystring = require('lib/vendor/querystring');
	
var Sdb = function(awsAccessKey, awsSecretKey) { 
	this.params = {};
	
	this._awsAccessKey = this.params.AWSAccessKeyId = awsAccessKey;	
	this._awsSecretKey = awsSecretKey;
	
	this.params.Version = "2009-04-15";
	this.params.SignatureVersion = '2';
	this.params.SignatureMethod = 'HmacSHA256';
	
	this.retries = 0;
	
	this.awsUrl = 'http://sdb.amazonaws.com';
}

Sdb.prototype = new EventEmitter;
Sdb.prototype.constructor = Sdb;

Sdb.prototype.listDomains = function() {
	return this.request('ListDomains');
};

Sdb.prototype.ListDomainsSuccess = function(response) { 
	this.responseMetadata.NextToken = response.NextToken;
	
	return this.emit('success', response.ListDomainsResult.DomainName, 
		this.responseMetadata);
}

Sdb.prototype.domainMetadata = function(domainName) { 
	if (!domainName) throw new Error('no domain specified');
	
	this.params.DomainName = domainName;
	
	return this.request('DomainMetadata');
};

Sdb.prototype.DomainMetadataSuccess = function(response) {
	return this.emit('success', response.DomainMetadataResult, this.responseMetadata);
};

Sdb.prototype.createDomain = function(domainName) { 
	if (!domainName) throw new Error('no domain specified');
	
	this.params.DomainName = domainName;
	
	return this.request("CreateDomain");
};

Sdb.prototype.deleteDomain = function(domainName) { 
	if (!domainName) throw new Error('no domain specified');
	
	this.params.DomainName = domainName;
	
	return this.request('DeleteDomain'); 
};

Sdb.prototype.select = function(query, token) { 
	if (!query) throw new Error('no query specified');
	
	this.params.SelectExpression = 'select ' + query;
	
	if (token) this.params.NextToken = token;
	
	return this.request('Select');
};

Sdb.prototype.selectAll = function(query) { 
    var getAllResults = function(results, metadata) {
        if (results) for (var key in results) this.results[key] = results[key];
        
        var next = null;
        
        if (metadata) {            
            this.metadata.RequestIds.push(metadata.RequestId);
            this.metadata.BoxUsage += parseFloat(metadata.BoxUsage);
            this.metadata.RequestCompleteTime += parseInt(metadata.RequestCompleteTime);
            
            if (!metadata.NextToken || !metadata.NextToken.length) 
                return this.self.emit('success', this.results, this.metadata);
            
            next = metadata.NextToken;
        }
        
        var sdb = new Sdb(this.self._awsAccessKey, this.self._awsSecretKey);
    
        sdb.select(this.q, next).on('success', $.proxy(this.recurse, this));
    }
    
    $.proxy(getAllResults, {
        recurse: getAllResults, 
        self: this, 
        q: query,
        results: {},
        metadata: {
            BoxUsage: 0,
            RequestIds: [],
            RequestCompleteTime: 0
        }
    })();
    
    return this;
}

Sdb.prototype.SelectSuccess = function(response) { 
	var results = {}, attributes, select = response.SelectResult;
	
	this.responseMetadata.NextToken = select.NextToken || null;
	
	if (!select.Item) return this.emit('success', {}, this.responseMetadata);
	
	var asObject = function(item) { 		
		if (!$.isArray(item)) item = [item];
		
		for (var i = 0, obj = {}, pair; pair = item[i]; i++) {
			if ($.isArray(obj[pair.Name])) {
				obj[pair.Name].push(pair.Value);
			}
			else if (typeof(obj[pair.Name]) == 'undefined') {
				obj[pair.Name] = pair.Value;
			}
			else {
				obj[pair.Name] = [obj[pair.Name], pair.Value];
			}
		}
			
		return obj;
	}
	
	if (select.Item.Name) {		
		results[select.Item.Name] = asObject(select.Item.Attribute);
		
		return this.emit('success', results, this.responseMetadata);
	}
	
	for (var i = 0, item; item = response.SelectResult.Item[i]; i++) {
		results[item.Name] = asObject(item.Attribute);
	}
	
	return this.emit('success', results, this.responseMetadata);
};

Sdb.prototype.deleteAttributes = function(domainName, itemName, attributes) { 
	if (!domainName || !itemName) throw new Error('invalid domainName / itemName');
	
	this.params.DomainName = domainName;
	this.params.ItemName = itemName;
	
	if (!attributes) return this.request('DeleteAttributes');
	
	var i = 0;
	
	for (name in attributes) {
		this.params['Attribute.' + i + '.Name'] = name.toString();
	}
	
	return this.request('DeleteAttributes');
};

Sdb.prototype.niceValue = function(val) {
	if (typeof(val) == 'object' && // is this a date? format for SDB
		/^function.Date\(\)/.test(val.constructor.toString())) {
			
		return (function(d) {
			var pad = function(n) { return (n < 10) ? '0' + n : n };
			
	 		return d.getUTCFullYear() + '-'	+ pad(d.getUTCMonth() + 1) + '-'
				+ pad(d.getUTCDate()) + 'T'	+ pad(d.getUTCHours()) + ':'
				+ pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()) + 'Z'
		})(val);
	}
	
	return val.toString();
}

Sdb.prototype.sdbAttributes = function(attributes) {
	var attribute, result = [];
	
	for (name in attributes) { 
		attribute = {};
		
		if (!$.isArray(attributes[name])) {
			attributes[name] = [attributes[name]];
 			attribute.replace = 'true';
		}
		else if ($.isArray(attributes[name][0])) {
			attributes[name] = attributes[name][0];
			attribute.replace = 'true';
		}
		
		for (var i = 0, ii = attributes[name].length; i < ii; i++) {
			attribute.name = name.toString();
			attribute.value = this.niceValue(attributes[name][i]);

			result.push($.extend({}, attribute));
		}
	}
	
	return result;
};

// exactly like putAttributes but items is an object instead
Sdb.prototype.batchPutAttributes = function(domainName, items) {
	if (!domainName || !items) throw new Error('invalid domainName / items');	
	
	this.params.DomainName = domainName;

	var i = 0, itemName, sdbAttributes;
	
	for (itemName in items) { 
		sdbAttributes = this.sdbAttributes(items[itemName]);
		
		this.params['Item.' + i + '.ItemName'] = itemName;
		
		for (var j = 0, attribute; attribute = sdbAttributes[j]; j++) { 
			this.params['Item.' + i + '.Attribute.' + j + '.Name'] = attribute.name;
			this.params['Item.' + i + '.Attribute.' + j + '.Value'] = attribute.value;
			
			if (attribute.replace) { 
				this.params['Item.' + i + '.Attribute.' + j + '.Replace'] = 'true';
			}
		}
		
		i++;
	}
	
	if (i > 25) throw new Error('too many items for a batch put request');
	
	return this.request('BatchPutAttributes');
};

// attributes is an array of name/value/values. val: single or [[]] means don't append
Sdb.prototype.putAttributes = function(domainName, itemName, attributes) {
	if (!domainName || !itemName) throw new Error('invalid domainName / itemName');
	
	if ($.isPlainObject(itemName)) 
		return this.batchPutAttributes(domainName, itemName); // this is a batch put req
	
	this.params.DomainName = domainName;
	this.params.ItemName = itemName;
		
	var sdbAttributes = this.sdbAttributes($.extend(true, {}, attributes));
	
	for (var i = 0, attribute; attribute = sdbAttributes[i]; i++) {
		this.params['Attribute.' + i + '.Name'] = attribute.name;
		this.params['Attribute.' + i + '.Value'] = attribute.value;
		
		if (attribute.replace) this.params['Attribute.' + i + '.Replace'] = 'true';
	}
	
	return this.request('PutAttributes');
};

Sdb.prototype.put = Sdb.prototype.putAttributes;

Sdb.prototype.getAttributes = function(domainName, itemName) {
	if (!domainName || !itemName) throw new Error('no item specified');
	
	this.params.DomainName = domainName;
	this.params.ItemName = itemName;
	
	return this.request('GetAttributes');
};

Sdb.prototype.get = Sdb.prototype.getAttributes;

Sdb.prototype.GetAttributesSuccess = function(response) {
	if ($.isEmptyObject(response.GetAttributesResult)) // 404
		return this.emit('success', null, this.responseMetadata);
	
	var result = response.GetAttributesResult.Attribute, attributes = {};
	
	if (!$.isArray(result)) result = [result];
	
	// turn the resulting array into an object. multi-vals become arrays
	// note: even if multi-val is 'intended' there is no way to know it should be an array
	for (var i = 0, name, value, ii = result.length; i < ii; i++) {
		name = result[i].Name; 
		value = result[i].Value;
		
		if (typeof(attributes[name]) == 'undefined') {
			attributes[name] = value;
		}
		else if ($.isArray(attributes[name])) {
			attributes[name].push(value);
		}
		else {
			attributes[name] = [value, attributes[name]];
		}
	}
	
	return this.emit('success', attributes, this.responseMetadata);
};

Sdb.prototype.request = function(action) { 
    var self = this;
    
	if (!action && !this.params.Action) throw new Error('no type of request specified')
		
    self.params.Action = this.params.Action || action;
	self.params.Timestamp = new Date(Math.round(new Date().getTime() / 1000) * 1000).toISOString();
	self.params.Signature = this.getSignature();
	
	var body = querystring.stringify(self.params);

    var options = {
        host: 'sdb.amazonaws.com',
        port: 80,
        path: '/',
        method: 'POST',
        headers: {
            'Content-Length': body.length,
    		'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
         	'Host': "sdb.amazonaws.com",
        }
    };
    
    var request = http.request(options, function(response) {         
        self.processResponse(response);
    }).
    on('error', function(err) { 
        console.log(err); 
    });
    
    request.write(body); 
    
    request.end();
    	
	if (!this.requestStart) self.requestStart = new Date().getTime();
		
	return self;
};

Sdb.prototype.processResponse = function(response) { 
	var data = '';
	
	response.on('data', function(chunk) { 
		data += chunk;
	});
	
	response.on('end', $.proxy(function(msg) { 
		var xmlParser = new xml2js.Parser();
				
		xmlParser.on('end', $.proxy(function(result) {									
			var now = new Date().getTime();
			
			this.emit('complete', result);
			
			if (!result || result.Errors) return this.failure(result);
			
			this.responseMetadata = result.ResponseMetadata;
			
			this.responseMetadata.RequestCompleteTime = now - this.requestStart;
			
			var success = this[this.params.Action + 'Success'];
			
			return (success) ? $.proxy(success, this)(result) : 
				this.emit('success', null, this.responseMetadata)

		}, this));
		
		xmlParser.parseString(data); 			    						
	}, this))
};

Sdb.prototype.failure = function(result) {
	var error = {};
		
	if (result && result.Errors && result.Errors.Error) {
		error = {
			code : result.Errors.Error.Code,
			message : result.Errors.Error.Message
		};
		
        if (this.retries < 5)
            if (error.code == 'ServiceUnavailable' ||
                error.code == 'InternalError') 
                return setTimeout($.proxy(this.request, this),
                                  1000 * Math.pow(2, this.retries++));

		this.responseMetadata = {
            Retries : this.retries,
			BoxUsage : result.Errors.Error.BoxUsage,
			RequestId : result.RequestId
		}
	} else {
		error = {
			code : 'NoResponse',
			message : 'SimpleDb did not respond at all.'
		}
	}
	
	return this.emit('failure', error, this.responseMetadata);
}

Sdb.prototype.getSignature = function() { 
    var query = this.params,
        keys = [],
        sorted = {};

    for (var key in query) keys.push(key);

    keys = keys.sort();

    for (var k in keys) {
        var key = keys[k];
        sorted[key] = query[key];
    }

    var stringToSign = ["POST", "sdb.amazonaws.com", '/', querystring.stringify(sorted)].join("\n");
    
    stringToSign = stringToSign.replace(/'/gm, "%27");
    stringToSign = stringToSign.replace(/\*/gm, "%2A");
    stringToSign = stringToSign.replace(/\(/gm, "%28");
    stringToSign = stringToSign.replace(/\)/gm, "%29");
    stringToSign = stringToSign.replace(/!/gm, '%21');
    
    var hash = crypto.createHmac("sha256", this._awsSecretKey);

    return hash.update(stringToSign.toString()).digest("base64");
};

exports.Sdb = Sdb;