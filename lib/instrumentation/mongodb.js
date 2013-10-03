"use strict";

var path            = require('path')
  , ParsedStatement = require(path.join(__dirname, '..', 'db', 'parsed-statement'))
  , shimmer         = require(path.join(__dirname, '..', 'shimmer'))
  , logger          = require(path.join(__dirname, '..',
                                        'logger')).child({component : 'mongodb'})
  , MONGODB         = require(path.join(__dirname, '..', 'metrics', 'names')).MONGODB
  ;

/**
 * Wrap each, because in most read queries it's the end point of the database
 * call chain.
 *
 * @param {TraceSegment} segment The current segment, to be closed when done.
 * @param {Agent} agent The currently active agent.
 *
 * @returns {Function} A callback that further wraps the callback called by the
 *                     wrapped each method, so we can tell when the cursor is
 *                     exhausted.
 */
function wrapNextObject(segment, tracer) {
	return function(nextObject) {
		return function(){
			var cursor = this;
			var args = Array.prototype.slice.call(arguments);
			  var callback = args.pop();
			  
			  if (typeof(callback) == 'function') {
				  args.push(tracer.callbackProxy(function(err,object){
					  
					  if (segment) {
						  var limit = cursor.limitValue == -1 ? 1 : cursor.limitValue;
						 if (limit == cursor.totalNumberOfRecords && cursor.items.length == 0) {
							 segment.end();
							 segment = null;
					         //logger.trace("MongoDB query trace segment ended (end of batch). %s ", cursor.collection.collectionName);
						 }
						 else if (!object ) {
							 segment.end();
							 segment = null;
					        //logger.trace("MongoDB query trace segment ended.(null object). %s ", cursor.collection.collectionName);
						 } 
					  }
					 return callback.apply(this,arguments);
				  }));
				  return nextObject.apply(this,args);
			  }
			  else {
				  // nothing to do
				  return nextObject.apply(this,arguments);
			  }
		}
	}
}

function addMongoStatement(state, collection, operation) {
  var statement = new ParsedStatement(operation, collection)
    , recorder  = statement.recordMetrics.bind(statement)
    , name      = MONGODB.PREFIX + collection + '/' + operation
    , next      = state.getSegment().add(name, recorder)
    ;

  state.setSegment(next);

  return next;
}

module.exports = function initialize(agent, mongodb) {
  if (!(mongodb && mongodb.Collection && mongodb.Collection.prototype)) return;

  var tracer = agent.tracer;

  // find
  shimmer.wrapMethod(mongodb.Collection.prototype, 'mongodb.Collection.prototype',
                     'find', function (command) {
    return tracer.segmentProxy(function () {
      var state      = tracer.getState()
        , collection = this.collectionName || 'unknown'
        , terms      = typeof arguments[0] === 'function' ? undefined : arguments[0]
        ;

      if (!state || arguments.length < 1) {
        logger.trace("Not tracing MongoDB %s.find(); no transaction or parameters.",
                     collection);
        if (terms) logger.trace({terms : terms}, "With terms:");

        return command.apply(this, arguments);
      }

      logger.trace("Tracing MongoDB %s.find(%j).", collection, terms);
     
      var segment = addMongoStatement(state, collection, 'select');
      if (typeof terms === 'object') segment.parameters = terms;

      var callback = arguments[arguments.length - 1];
      if (typeof callback !== 'function') {
        // no callback, so wrap the cursor iterator
        var cursor = command.apply(this, arguments);
        shimmer.wrapMethod(cursor, 'cursor', 'nextObject', wrapNextObject(segment, tracer));

        return cursor;
      }
      else {
        // FIXME: the proxied callback closes over too much state to extract
        var args = Array.prototype.slice.call(arguments, 0, -1);
        args.push(tracer.callbackProxy(function (err,cursor) {
        	
        	if (cursor) {
        		shimmer.wrapMethod(cursor, 'cursor', 'nextObject', wrapNextObject(segment, tracer));
        	}
        	
        	var returned = callback.apply(this, arguments);

        	//segment.end();
        	//logger.trace("Tracing MongoDB %s.%s(%j) ended for transaction %s.",collection, 'find', terms, state.getTransaction().id);
          
        	//console.trace('ended trace');

          return returned;
        }));

        return command.apply(this, args);
      }
    });
  });

  // try to map to a SQL statement
  [ {n:'insert', op:'insert'}, 
    {n:'update', op:'update'},
	{n:'remove', op:'delete'},
	{n:'ensureIndex', op:'select'},
	{n:'count', op:'select'},
	{n:'findAndModify', op:'update'}
  ].forEach(function (info) {
	  var operation = info.n;
	  
    shimmer.wrapMethod(mongodb.Collection.prototype,
                       'mongodb.Collection.prototype', operation, function (command) {
      return tracer.segmentProxy(function () {
        var state      = tracer.getState()
          , collection = this.collectionName || 'unknown'
          , args       = Array.prototype.slice.call(arguments)
          , terms      = typeof args[0] === 'function' ? undefined : args[0]
          ;

        if (!state || args.length < 1) {
          logger.trace("Not tracing MongoDB %s.%s(); no transaction or parameters.",
                       collection, operation);
          if (terms) logger.trace({terms : terms}, "With terms:");

          return command.apply(this, arguments);
        }

        logger.trace("Tracing MongoDB %s.%s(%j).", collection, operation, terms);

        var segment = addMongoStatement(state, collection, info.op);
        if (typeof terms === 'object') segment.parameters = terms;

        var callback = args.pop();
        if (typeof callback !== 'function') {
          args.push(callback);
          // add ours
          args.push(tracer.callbackProxy(function () {
            segment.end();
            logger.trace("Tracing MongoDB %s.%s(%j) ended for transaction %s.",
                         collection, operation, terms, state.getTransaction().id);
          }));
        }
        else {
          // FIXME: the proxied callback closes over too much state to extract
          args.push(tracer.callbackProxy(function () {
         
            segment.end();
            logger.trace("Tracing MongoDB %s.%s(%j) ended for transaction %s.",
                         collection, operation, terms, state.getTransaction().id);

            return callback.apply(this, arguments);
          }));
        }

        return command.apply(this, args);
      });
    });
  });
};
