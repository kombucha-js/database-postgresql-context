'use strict';

// Object.assign( module.exports,  require( 'asynchronous-context' ));

require('dotenv').config();
const { AsyncContext }     = require( 'asynchronous-context/context' );
const { preventUndefined,unprevent } = require( 'prevent-undefined' );
const sqlNamedParameters   = require( 'sql-named-parameters' );

const { Pool, Client } = require('pg')

const {
  DatabaseContextError,
  DatabaseContextDataset,
  DatabaseContextMultipleDataset,
} = require( './datasets' );


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


const result2dataset = result => new DatabaseContextDataset( result .rows ?? null, result.rowCount ?? null);

const result2log = (result)=>({
    rows      :  result.rows,
    count     :  result.count,
});

async function __query( sql, numberedParams, namedParams ) {
  try {
    if ( ! this.isConntected() )
      throw new Error( 'no database connection was established' );

    const result = await this.__pgClient.query( sql, numberedParams );

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
      return result2dataset( result );
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

      return new DatabaseContextMultipleDataset( result.map( result2dataset ));
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


const SQL_BEGIN    = `BEGIN;`;
const SQL_COMMIT   = `COMMIT;`;
const SQL_ROLLBACK = `ROLLBACK;`;


async function shutdownDatabaseContext() {
  await end();
  return true;
}
DatabaseContext.shutdownDatabaseContext = shutdownDatabaseContext;

// >>> ADDED (Tue, 18 Apr 2023 10:10:55 +0900)
module.exports.shutdownDatabaseContext = shutdownDatabaseContext;
// <<<


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

