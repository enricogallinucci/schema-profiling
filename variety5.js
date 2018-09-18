// This var is not used, it is simply a reference to the name used for custom fields
var projectName = 'bdcop'; //BD-COP Big Data COntextual Profiler

(function () { 'use strict'; // wraps everything for which we can use strict mode -JC

	var log = function(message) {
	  if(!__quiet) { // mongo shell param, coming from https://github.com/mongodb/mongo/blob/5fc306543cd3ba2637e5cb0662cc375f36868b28/src/mongo/shell/dbshell.cpp#L624
		  print(message);
		}
	};

	var dbs = [];
	var emptyDbs = [];

	// Check database
	var knownDatabases = db.adminCommand('listDatabases').databases;
	if(typeof knownDatabases !== 'undefined') { // not authorized user receives error response (json) without databases key
	  knownDatabases.forEach(function(d){
		if(db.getSisterDB(d.name).getCollectionNames().length > 0) {
		  dbs.push(d.name);
		}
		if(db.getSisterDB(d.name).getCollectionNames().length === 0) {
		  emptyDbs.push(d.name);
		}
	  });

	  if (emptyDbs.indexOf(db.getName()) !== -1) {
		throw 'The database specified ('+ db +') is empty.\n'+
			  'Possible database options are: ' + dbs.join(', ') + '.';
	  }

	  if (dbs.indexOf(db.getName()) === -1) {
		throw 'The database specified ('+ db +') does not exist.\n'+
			  'Possible database options are: ' + dbs.join(', ') + '.';
	  }
	}

	// Check collection
	var collNames = db.getCollectionNames().join(', ');
	if (typeof collection === 'undefined') {
	  throw 'You have to supply a \'collection\' variable, à la --eval \'var collection = "animals"\'.\n'+
			'Possible collection options for database specified: ' + collNames + '.\n'+
			'Please see https://github.com/variety/variety for details.';
	}

	if (db[collection].count() === 0) {
	  throw 'The collection specified (' + collection + ') in the database specified ('+ db +') does not exist or is empty.\n'+
			'Possible collection options for database specified: ' + collNames + '.';
	}

	// Set configuration (setting limit to #docs, analyzing arrays, updating docs with schema info, etc.)
	var readConfig = function(configProvider) {
	  var config = {};
	  var read = function(name, defaultValue) {
		var value = typeof configProvider[name] !== 'undefined' ? configProvider[name] : defaultValue;
		config[name] = value;
	  };
	  
	  var currentdate = new Date(); 
	  var datetime = "Start time: " + currentdate.getDate() + "/"
					+ (currentdate.getMonth()+1)  + "/" 
					+ currentdate.getFullYear() + " @ "  
					+ currentdate.getHours() + ":"  
					+ currentdate.getMinutes() + ":" 
					+ currentdate.getSeconds();
	  log(datetime);

	  read('collection', null);
	  read('query', {});
	  read('limit', db[config.collection].find(config.query).count());
	  read('collapse', true);
	  read('cleanDocs', false);
	  read('saveDocs', true);
	  read('maxDepth', 99);
	  read('sort', {_id: -1});
	  read('outputFormat', 'ascii');
	  read('persistResults', false);
	  
	  config.dumpCollection = 'DumpOf' + config.collection;
	  db[config.dumpCollection].drop();
	  
	  return config;
	};

	var config = readConfig(this);

	// Setting up plugins?
	var PluginsClass = function(context) {
	  var parsePath = function(val) { return val.slice(-3) !== '.js' ? val + '.js' : val;};
	  var parseConfig = function(val) {
		var config = {};
		val.split('&').reduce(function(acc, val) {
		  var parts = val.split('=');
		  acc[parts[0]] = parts[1];
		  return acc;
		}, config);
		return config;
	  };

	  if(typeof context.plugins !== 'undefined') {
		this.plugins = context.plugins.split(',')
		  .map(function(path){return path.trim();})
		  .map(function(definition){
			var path = parsePath(definition.split('|')[0]);
			var config = parseConfig(definition.split('|')[1] || '');
			context.module = context.module || {};
			load(path);
			var plugin = context.module.exports;
			plugin.path = path;
			if(typeof plugin.init === 'function') {
			  plugin.init(config);
			}
			return plugin;
		  }, this);
	  } else {
		this.plugins = [];
	  }

	  this.execute = function(methodName) {
		var args = Array.prototype.slice.call(arguments, 1);
		var applicablePlugins = this.plugins.filter(function(plugin){return typeof plugin[methodName] === 'function';});
		return applicablePlugins.map(function(plugin) {
		  return plugin[methodName].apply(plugin, args);
		});
	  };

	  // log('Using plugins of ' + tojson(this.plugins.map(function(plugin){return plugin.path;})));
	};

	var $plugins = new PluginsClass(this);
	$plugins.execute('onConfig', config);

	
	// Function that returns the type of a thing
	/*var varietyTypeOf = function(thing) {
	  if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }

	  if (typeof thing !== 'object') {
	    var t = typeof thing;
		// if(t == 'number' || t == 'boolean' || t == 'string') return t;
		// else return 'string';
		return 'primitive';
	  }
	  else {
		if (thing && thing.constructor === Array) {
		  return 'array';
		}
		else if (thing === null) {
		  //return 'null';
		  return 'primitive';
		}
		else if (thing instanceof Date) {
		  // return 'Date';
		  return 'primitive';
		}
		else if (thing instanceof ObjectId) {
		  // return 'ObjectId';
		  return 'primitive';
		}
		else if (thing instanceof BinData) {
		  // return 'string';
		  return 'primitive';
		} else {
		  return 'object';
		}
	  }
	};*/
	
	var varietyTypeOf = function(thing) {
	  if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }

	  var ret = {};
	  ret.value = thing;
	  
	  if (typeof thing !== 'object') {
	    ret.type = 'primitive';
		if(typeof thing === 'string')
			// ret.value = thing.replace(/[^\x00-\x7F]/g, "").replace(/(\r\n|\n|\r)/gm,"").replace(",","");
			ret.value = thing.replace(/[^a-z0-9]/gi,'');
	  }
	  else {
		if (thing && thing.constructor === Array) {
		  ret.type = 'array';
		}
		else if (thing === null) {
		  // ret.value = 'bdcop_NA';
		  ret.type = 'primitive';
		}
		else if (thing instanceof Date) {
		  ret.value = thing.getTime();
		  ret.type = 'primitive';
		}
		else if (thing instanceof ObjectId) {
		  // ret.value = thing.str.replace(/[^\x00-\x7F]/g, "").replace(/(\r\n|\n|\r)/gm,"");
		  ret.type = 'skip';
		}
		else if (thing instanceof BinData) {
		  ret.type = 'skip';
		  // ret.value = hex2a(thing.hex()).replace(/[^\x00-\x7F]/g, "").replace(/(\r\n|\n|\r)/gm,"");
		} else {
		  ret.type = 'object';
		}
	  }
	  return ret;
	};
	
	var hex2a = function(hex) {
		var str = '';
		for (var i = 0; i < hex.length; i += 2)
			str += String.fromCharCode(parseInt(hex.substr(i, 2), 16));
		return str;
	};

	// Returns the most specific supertype
	// object // object - array - string // string - number - boolean - null - Date
	var mergeTypes = function(type1,type2){
		if(type1 == type2){
			return type1;
		}
		else if( type1=='object' || type2=='object' || type1=='array' || type2=='array'){
			return 'object';
		}
		else {
			return 'primitive';
		}
	}

	// Returns true, if object in argument may have nested objects and makes sense to analyse its content
	function isHash(v) {
		var isArray = Array.isArray(v);
		var isObject = typeof v === 'object';
		var specialObject = v instanceof Date ||
						v instanceof ObjectId ||
						v instanceof BinData;
		return !specialObject && (isArray || isObject);
	}
	  
	// Returns true if n is a number; used to identify elements of arrays
	function isNumeric(n) {
	  return !isNaN(parseFloat(n)) && isFinite(n);
	}
	
	// THE function that recursively extracts the schema
	var analyseDocument2 = function(document, maxDepth) {

	  var analyzeKey = function(document, maxDepth, key, result){
		//skip over inherited properties such as string, length, etch
		if(!document.hasOwnProperty(key) || key=='_id' || key=='bdcop_schemaId' || key=='bdcop_clusterId' || key=='clusterId') {
			return;
		}
		var value = document[key];
		if(isNumeric(key)){
			key = 'XX';
			print('Shouldn\'t be here (inside array)!');
			return;
		}
		var vto = varietyTypeOf(value);
		
		//if(type!="null"){
			// if(type == 'Date'){
				// type = 'number';
				// document[key] = value.getTime();
			// }
			// else if(type == 'string' && config.cleanDocs){
			
			// delete document[key];
			
			if(vto.type == 'skip'){
				return;
			}
			else if(vto.type == 'primitive'){
				var docKey = key + '_' + vto.type;
				delete document[key];
				document[docKey] = vto.value;
				// document[key] = vto.value;
				// document[key] =  = value.toString().match(/^[0-9]+\.([0-9]+)$/);
			}	
			else if(vto.type == 'array'){
				delete document[key];
			}
			
			if(typeof result[key] === 'undefined') {
				result[key] = {
					bdcop_type: vto.type
				};
			}
			else{
				result[key].bdcop_type = mergeTypes(result[key].bdcop_type,vto.type);
				print('Shouldn\'t be here (merging types)!');
				// if(key == 'XX' && ( result[key].bdcop_type == 'object' || result[key].bdcop_type == 'array' ) ) return; 
			}
			
			//if it's an object, recurse...only if we haven't reached max depth
			if(vto.type == 'object' && maxDepth > 1) {
				var children = analyseDocument2(value, maxDepth-1);
				var nChildren = children ? Object.keys(children).length : 0;
				// var str = 'Analizzo '+key+' di tipo '+type+': di figli ne ha '+nChildren;
				if(nChildren>0){
					Object.keys(children).sort().forEach(function(attrname) {
						result[key][attrname] = children[attrname]; 
					});
				}
				// print(str);
			}
		//}
	  }

	  var result = {};
	  if (typeof document !== 'object'){
		Object.keys(document).sort().forEach(function(key) {
			analyzeKey(document, maxDepth, key, result);
		});
	  }
	  else{
	  	for(var key in document){
	  		analyzeKey(document, maxDepth, key, result);
	  	}
	  }
	  
	  return result;
	}

	// Function that builds the schema in RTED format (Recursive Tree Edit Distance)
	var rtedFormat = function(docResult) {
		
		var iterate = function(doc){
			var str = "";
			for(var key in doc) {
				var value = doc[key];
				if(key == "bdcop_type"){
					str += " { " + value + " }";
				}
				else{
					str += " { " + key;
					var vto = varietyTypeOf(value);
					if(vto.type=='object'){
						str += iterate(value);
					}
					else{
						str += " { " + value + " }";
					}
					str += " }";
				}
			}
			return str;
		}
		
		return "{ root " + iterate(docResult) + " }";
	}

	// Function that performes schema matching between one doc (docResult) and all the others (interimResults) and return the schemaID of the doc
	var mergeDocument = function(docResult, interimResults, _id) {
	  var found = false; var schemaId = 0;
	  var docResultStringified = JSON.stringify(docResult);
	  for (var i=0; i<interimResults.length; i++) {
		if(interimResults[i].serializedSchema == docResultStringified){
			found = true;
			schemaId = interimResults[i].bdcop_schemaId;
			interimResults[i].occurrences++;
		}
	  }
	  if(!found){
		var rted = rtedFormat(docResult);
		schemaId = interimResults.length;
		var ir = {
			bdcop_schemaId: schemaId,
			schema: docResult,
			serializedSchema: docResultStringified,
			rted: rted,
			occurrences: 1
		}
		interimResults.push(ir);
	  }
	  var schemaIdToSet = "S" + schemaId;
	  return schemaIdToSet;
	};

	// Function that saves the updated obkect in MongoDB; necessary to add the SchemaID and to replace values, 
	// such as cleaned strings and dates converted to numbers
	var updateObj = function(obj) {
		// var updateStr = JSON.stringify(obj);
		db[config.dumpCollection].insert(obj);
		// db[config.collection].save(obj);
	};
	
	var addSchemaFeatures = function(object, schema){
		//TODO scorri l'object e togli tutti gli obj inutili?
		
		//TODO scorri lo schema e aggiungi tutte le features all'object
		var iterate = function(doc, parentKey){
		
			if(parentKey != "exists_"){
				var k = parentKey + "_" + doc['bdcop_type'];
				object[k] = 1;
			}
		
			if(Object.keys(doc).length>1){
				for(var key in doc) {
					if(key == 'bdcop_type') continue;
					
					var newKey = parentKey == "exists_" ? parentKey+key : parentKey+"-"+key;
					iterate(doc[key],newKey);
				}
			}
		}
		
		iterate(schema, "exists_");
	}
	
	// Creates the mongoexport string
	var strMongoExport = function(schemas){
		
		var iterate = function(doc, features, parentKeyVal, parentKeySch){
		
			if(parentKeyVal != ""){
				var k = "exists_" + parentKeySch + "_" + doc['bdcop_type'];
				if(features.sch.indexOf(k)==-1) features.sch.push(k);
				
				if(doc['bdcop_type']=='primitive'){
					var k1 = parentKeyVal + "_primitive";
					if(features.val.indexOf(k1)==-1) features.val.push(k1);
				}
			}
		
			for(var key in doc) {
				if(key == 'bdcop_type') continue;
				
				var newParentKeyVal = parentKeyVal == "" ? parentKeyVal+key : parentKeyVal+"."+key;
				var newParentKeySch = parentKeySch == "" ? parentKeySch+key : parentKeySch+"-"+key;
				iterate(doc[key], features, newParentKeyVal, newParentKeySch);
			}
		};
		
		var features = { val: [], sch: [] };
		for (var i = 0; i < schemas.length; i++) {
			iterate(schemas[i].schema, features, "", "");
		}
		
		// for (var i = 0; i < schemas.length; i++) {
			// for(var key in schemas[i].schema){
				// if (key.substring(0, 8) == "exists_"){
					// if(features.sch.indexOf(key)==-1) features.sch.push(key);
				// }
				// else{
					// if(features.val.indexOf(key)==-1) features.val.push(key);
				// }
			
			// }
		// }

		var fields = "";
		for (var i = 0; i < features.val.length; i++) {
			fields += "," + features.val[i];
		};
		for (var i = 0; i < features.sch.length; i++) {
			fields += "," + features.sch[i];
		};
		
		print("mongoexport --port 27020 --db " + db.getName() + " --collection " + config.dumpCollection + " --csv --fields bdcop_schemaId" + fields + " --out /home/egal/json-parsing/dataset" + config.collection + ".csv");
	}

	// Reduce function for each document: analyze, merge and update
	var reduceDocuments = function(accumulator, object) {
	  var docResult = analyseDocument2(object, config.maxDepth);
	  object.bdcop_schemaId = mergeDocument(docResult, accumulator, object._id);
	  addSchemaFeatures(object,docResult);
	  if(config.saveDocs){
		updateObj(object);
	  }
	  return accumulator;
	};

	// Not sure: extend standard MongoDB cursor of reduce method - call forEach and combine the results
	DBQuery.prototype.reduce = function(callback, initialValue) {
	  var result = initialValue;
	  this.forEach(function(obj){
		result = callback(result, obj);
	  });
	  return result;
	};

	// Get the documents
	var cursor = db[config.collection].find(config.query).sort(config.sort).limit(config.limit);
	var schemas = new Array();
	
	// Launch the procedure
	var varietyResults = cursor.reduce(reduceDocuments, schemas); //sostituisco {} con [] e interimResults con varietyResults
	
	strMongoExport(schemas);

	// Store results
	var resultsDB = db.getMongo().getDB('varietyResults');
	var resultsCollectionName = collection + 'Keys';

	log('creating results collection: '+resultsCollectionName);
	resultsDB[resultsCollectionName].drop();
	resultsDB[resultsCollectionName].insert(varietyResults);
	  
	var currentdate = new Date(); 
	var datetime = "End time: " + currentdate.getDate() + "/"
								+ (currentdate.getMonth()+1)  + "/" 
								+ currentdate.getFullYear() + " @ "  
								+ currentdate.getHours() + ":"  
								+ currentdate.getMinutes() + ":" 
								+ currentdate.getSeconds();
	log(datetime);


}.bind(this)()); // end strict mode
