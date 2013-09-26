'use strict';

var path    = require('path')
  , shimmer = require(path.join(__dirname, '..', 'shimmer'))
  , logger  = require(path.join(__dirname, '..', 'logger')).child({component : 'connect'})
  ;

/*
 *
 * CONSTANTS
 *
 */

var ORIGINAL = '__NR_original';
var RESERVED = [ // http://es5.github.io/#x7.6.1.2
  // always (how would these even get here?)
  'class', 'enum', 'extends', 'super', 'const', 'export', 'import',
  // strict
  'implements', 'let', 'private', 'public', 'yield', 'interface',
  'package', 'protected', 'static'
];

/**
 * ES5 strict mode disallows some identifiers that are allowed in non-strict
 * code. Mangle function names that are on that list of keywords so they're
 * non-objectionable in strict mode (which is currently enabled everywhere
 * inside the agent, as well as at many customer sites).
 *
 * If you really need to crawl your Express app's middleware stack, change
 * your test to use name.indexOf('whatever') === 0 as the predicate instead
 * of name === 'whatever'. It's a little slower, but you shouldn't be doing
 * that anyway.
 *
 * @param {string} name The candidate function name
 *
 * @returns {string} A safe (potentially mangled) function name.
 */
function mangle(name) {
  if (RESERVED.indexOf(name) !== -1) return name + '_';

  return name;
}

module.exports = function initialize(agent, connect) {
	
  var tracer = agent.tracer;
	
  function wrapUse(use) {
	  return function(){
		  var args = Array.prototype.slice.call(arguments);
		  var h = args.pop();
		  
		  if (h.length < 4) {		  
			  args.push(tracer.callbackProxy(function(req,res,next){				  
				  // call the handler with the callback proxy
				  h(req,res, tracer.callbackProxy(function(err){
					  return next(err);
				  }) );
			  }));
		  }
		  else {
			  // error handler
			  args.push(function(error,req,res,next){
				  if (error) {
				        var transaction = agent.tracer.getTransaction();
				        if (transaction) {
				          transaction.exceptions.push(error);
				        }
				        else {
				          agent.errors.add(null, error);
				        }
				      }
				  
				  // pass it up
				  return h(error,req,res,next); 
			  });
		  }	  
		  
		  // return the app for chaining
		  return use.apply(this, args);
	  }
  }
  
  /**
   * Connect 1 and 2 are very different animals, but like Express, it mostly
   * comes down to factoring.
   */
  var version = connect && connect.version && connect.version[0];
  switch (version) {
    case '1':
      shimmer.wrapMethod(connect && connect.HTTPServer && connect.HTTPServer.prototype,
                         'connect.HTTPServer.prototype',
                         'use',
                         wrapUse);
      break;

    case '2':
      shimmer.wrapMethod(connect && connect.proto,
                         'connect.proto',
                         'use',
                         wrapUse);
      break;

    default:
      logger.warn("Unrecognized version %s of Connect detected; not instrumenting.",
                  version);
  }
};
