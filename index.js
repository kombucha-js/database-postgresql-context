'use strict';

// Object.assign( module.exports,  require( 'asynchronous-context' ));

const { AsyncContext }     = require( 'asynchronous-context/context' );
const { preventUndefined,unprevent } = require( 'prevent-undefined' );
const sqlNamedParameters   = require( 'sql-named-parameters' );

const { Pool, Client } = require('pg')
const pool = new Pool();
const DEBUG = false;

// the pool will emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool.on('error', (err, client) => {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

async function end() {
  await pool.end();
  return;
};

module.exports.end = end;

class DatabaseContextError extends Error {
  constructor( messageObject, options, ...args) {
    super( messageObject.message, options, ...args );
    this.messageObject = messageObject || {};
  }
}
module.exports.DatabaseContextError = DatabaseContextError;

class DatabaseContextDataset {
  constructor( result ) {
    if ( ! result  )
      throw new DatabaseContextError( { message: 'result was null' }  );

    if ( Array.isArray( result ) ) {
      throw new DatabaseContextError( { message: 'multiple resultset error'} );
    }

    this.result = result;
  }
  getResult() {
    return this.result;
  }
  get rowCount() {
    if ( typeof this.result.rowCount  === 'number' ) {
      return this.result.rowCount;
    } else {
      throw new DatabaseContextError({message:'the query was not an update query'});
    }
  }
  get rows() {
    if ( this.result.rows && Array.isArray( this.result.rows ) ) {
      return this.result.rows;
    } else {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
  }
  firstRow() {
    if ( this.result.rows && Array.isArray( this.result.rows ) && 0 < this.result.rows.length ) {
      return this.result.rows[0];
    } else {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
  }
  firstRowOrNull() {
    try {
      this.firstRow();
    } catch ( e ) {
      if ( e instanceof DatabaseContextError ) {
        return null;
      } else {
        throw e;
      }
    }
  }
  singleRow() {
    if ( this.result.rows && Array.isArray( this.result.rows ) ) {
      if ( this.result.rows.length < 1 ) {
        throw new DatabaseContextError({message:'the result has no dataset'});
      } else if ( this.result.rows.length === 1 ) {
        return this.result.rows[0];
      } else {
        throw new DatabaseContextError({message:`NOT UNIQUE : the result has more than one rows ${this.result.rows.length} `});
      }
    } else {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
  }
  singleRowOrNull() {
    // console.log( 'this.result.rows.length', this.result.rows.length );
    if ( this.result.rows && Array.isArray( this.result.rows ) ) {
      if ( this.result.rows.length < 1 ) {
        return null;
      } else if ( this.result.rows.length === 1 ) {
        return this.result.rows[0];
      } else {
        throw new DatabaseContextError({message:`NOT UNIQUE : the result has more than one rows ${this.result.rows.length} `});
      }
    } else {
      return null;
    }
  }
  getResultArray() {
    throw new DatabaseContextError( MSG_SINGLE_RESULTSET_ERROR );
  }
}
module.exports.DatabaseContextDataset = DatabaseContextDataset;


const MSG_SINGLE_RESULTSET_ERROR   = 'single resultset error / cannot call a method for multiple resultsets. ';
const MSG_MULTIPLE_RESULTSET_ERROR = 'multiple resultset error / cannot call a method for single resultset. ( maybe you accidentally get multiple results )' ;
class DatabaseContextMultipleDataset {
  constructor( result ) {
    if ( ! result  )
      throw new DatabaseContextError( 'result was null' );

    if ( ! Array.isArray( result ) ) {
      throw new DatabaseContextError( 'the specified result object is not an array' );
    }

    this.result = result.map( e=>new DatabaseContextDataset( e ) );;
  }
  getResultArray() {
    return [ ...this.result ];
  }
  getResult() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get rowcount() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get rows() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  firstRow() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  firstRowOrNull() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  singleRow() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  singleRowOrNull() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
}
module.exports.DatabaseContextMultipleDataset = DatabaseContextMultipleDataset;




class DatabaseContext extends AsyncContext {
  ctor(...args) {
    this.contextInitializers.push(async function databaseContextInitializer() {
      await this.initializeContextOfDatabaseContext();
    });
    this.contextFinalizers.unshift(async function databaseContextFinalizer(is_successful) {
      await this.finalizeContextOfDatabaseContext(is_successful);
    });
  }
}
module.exports.DatabaseContext = DatabaseContext;


async function __query( sql, numberedParams, namedParams ) {
  try {
    if ( ! this.isConntected() )
      throw new Error( 'no database connection was established' );

    let result = await this.__pgClient.query( sql, numberedParams );

    const result2log = (result)=>({
        rows      :  result.rows,
        rowCount  :  result.rowCount,
    });

    if ( ! Array.isArray( result ) ) {
      this.logger.output({
        type        : 'query-succeeded',
        sql         : sql,
        namedParams : namedParams,
        params      : numberedParams,
        value  : {
          type      : 'single resultset',
          result    : result2log( result ),
        }
      },1);
      return new DatabaseContextDataset( result );
    } else {

      this.logger.output({
        type        : 'query-succeeded',
        sql         : sql,
        namedParams : namedParams,
        params      : numberedParams,
        value  : {
          type      : 'multiple resultset',
          result    : result.map( result2log ),
        }
      },1);

      return new DatabaseContextMultipleDataset( result );
    }

  } catch ( e ) {
    console.error(e);
    const ee = new DatabaseContextError(
      { message : `DatabaseContext Error: ${e.message}\n${sql}\n${ namedParams }` },
      { cause: e }
    );

    try {
      this.logger.output({
        type        : 'query-error',
        sql         : sql,
        namedParams : namedParams,
        params      : numberedParams,
        value : ee,
      },1);
    } catch (eee){
      console.error(eee);
    }

    throw ee;

    // (Tue, 13 Dec 2022 13:28:02 +0900)
    // if you throw the raised database error directly, the stacktrace will be
    // incorrect.
    // throw e;

    // If you wrap the raised error with a new error, cause is not shown by the
    // jest.
    // throw new DatabaseContextError( { message : `DatabaseContext Error: ${e.message}\n${sql}\n${ params }` }, { cause: e } );
  }
};

DatabaseContext.prototype.__query = __query;

async function query( query, params ) {
  const { transformedQuery, positionalParams } = sqlNamedParameters.transform({query, params});
  return await this.__query( transformedQuery, positionalParams, params );
  // See this ->->---------------------------------------------> ^^^^^^
  // Pass the original (named) params in order to output logs.
  // 20221018172852
}
DatabaseContext.prototype.query = query;


function isConntected() {
  return this.__pgClient != null;
}
DatabaseContext.prototype.isConntected = isConntected;

async function connect() {
  this.logger.output({
    type   : 'database-connect',
  });
  if ( this.isConntected() )
    throw new DatabaseContextError({message:'this context has already established a connection.'});

  this.__pgClient = new Client();
  this.__pgClient.connect();
  // this.__pgClient = await pool.connect();

  return this;
};
DatabaseContext.prototype.connect = connect;

async function disconnect() {
  this.logger.output({
    type   : 'database-disconnect',
  });
  if ( this.isConntected() ) {
    if ( 'end' in this.__pgClient ) {
      await this.__pgClient.end();
    } else if ( 'release' in this.__pgClient ) {
      await this.__pgClient.release();
    } else {
      console.error('__pgClient has not method to finalize');
    }
    this.__pgClient = null;
  }
  return this;
};
DatabaseContext.prototype.disconnect = disconnect;

async function initializeContextOfDatabaseContext() {
  // console.log( 'this.getOptions().autoCommit', this.getOptions().autoCommit );
  if ( this.getOptions().autoCommit === true ) {
    // console.log( 'autoCommit is true ' );
    const context = this;
    context.__autoCommit = true;
    await context.connect();
    await context.beginTransaction();
  };
}
DatabaseContext.prototype.initializeContextOfDatabaseContext = initializeContextOfDatabaseContext;

async function finalizeContextOfDatabaseContext(is_successful) {
  const context = this;
  if ( context.isConntected() ) {

    // console.log( 'context.__autoCommit', context.__autoCommit );

    if ( context.__autoCommit === true ) {
      try {
        if ( is_successful ) {
          context.logger.log( 'commit for finalization' );
          await context.commitTransaction();
        } else {
          context.logger.log( 'rollback for finalization' );
          await context.rollbackTransaction();
        }
      } catch ( e ) {
        console.error(e);
        // This caused duplicate log data. The methods should output log by themself
        // this.logger.output({
        //   type   : 'finalization',
        //   status : MSG_WARNING,
        //   value  : { message : '[NOT CRITICAL] an error was occured when cleaning up the current transaction 1', error : e  },
        // });
      }
    }

    // This is to ensures that the current connection is properly finalized.
    try {
      context.logger.log( 'disconnect for finalization' );
      if ( context.isConntected() ) {
        await context.disconnect( context );
      }
    } catch ( e ) {
      console.error(e);
      // This caused duplicate log data. The method should output log by itself.
      // this.logger.output({
      //   type   : 'finalization',
      //   status : MSG_WARNING,
      //   value  : { message : '[NOT CRITICAL] an error was occured when cleaning up the current transaction 2', error : e  },
      // });
    }
  }
}
DatabaseContext.prototype.finalizeContextOfDatabaseContext = finalizeContextOfDatabaseContext;

// This is really bad. This hides the super method on the parent class and
// report nothing. (Thu, 27 Oct 2022 19:39:37 +0900)
/// async function finalizeContext() {
/// }
/// DatabaseContext.prototype.finalizeContext = finalizeContext;


/**
 * ===================================================================
 *
 * TAG_CONNECTION
 *
 * ===================================================================
 */

const _sql = (strings,...values)=>{
  // console.log( strings, values );
  return (
    strings
    .map( (e,i)=> e + (values[i]||'').toString() )
    .join('')
    .split('\n')
    .map( (e)=> e.trim() )
    .join( '\n' )
    .trim()
  );
}
const SQL_BEGIN    = _sql`BEGIN;`;
const SQL_COMMIT   = _sql`COMMIT;`;
const SQL_ROLLBACK = _sql`ROLLBACK;`;

module.exports._sql = _sql;

async function shutdownPool() {
  await end();
  return true;
}
DatabaseContext.shutdownPool = shutdownPool;


async function beginTransaction() {
  try {
    this.logger.enter( 'beginTransaction' );
    return await this.query( SQL_BEGIN );
  } finally {
    this.logger.leave( 'beginTransaction' );
  }
}
DatabaseContext.prototype.beginTransaction = beginTransaction;

async function commitTransaction() {
  try {
    this.logger.enter( 'commitTransaction' );
    return await this.query( SQL_COMMIT );
  } finally {
    this.logger.leave( 'commitTransaction' );
  }
  this.trace( 'commitTransaction' );
}
DatabaseContext.prototype.commitTransaction = commitTransaction;

async function rollbackTransaction() {
  try {
    this.logger.enter( 'rollbackTransaction' );
    return await this.query( SQL_ROLLBACK );
  } finally {
    this.logger.leave( 'rollbackTransaction' );
  }
}
DatabaseContext.prototype.rollbackTransaction = rollbackTransaction;


module.exports = module.exports;

