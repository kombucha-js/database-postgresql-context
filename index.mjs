'use strict';

import { dotenvFromSettings } from 'asynchronous-context/env';
import { AsyncContext }       from 'asynchronous-context/context' ;
import sqlNamedParameters     from 'sql-named-parameters' ;
import { sqlmacro }           from 'sqlmacro' ;
import pg from 'pg'
const { Pool, Client } = pg;


import {
  DatabaseContextError,
  DatabaseContextDataset,
  DatabaseContextMultipleDataset,
} from  './datasets.mjs' ;


const pool = new Pool();
const DEBUG = false;

export const AUTO_COMMIT = "autoCommit";
export const AUTO_CONNECT = "autoConnect";

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

export { end };

class DatabaseContext extends AsyncContext {
  ctor(...args) {
    this.__pgClient = null;
    this.__connected = false;
    this.__autoCommit = false;
    this.__autoConnect = false;
    this.contextInitializers.push(async function databaseContextInitializer() {
      await this.initializeContextOfDatabaseContext();
    });
    this.contextFinalizers.unshift(async function databaseContextFinalizer(is_successful) {
      await this.finalizeContextOfDatabaseContext(is_successful);
    });
  }
}
export {  DatabaseContext };

DatabaseContext.prototype.__ensure_client = function __ensure_client() {
  if ( this.__pgClient === null ) {
    this.__pgClient = new Client();
  }
};


const result2dataset = result => new DatabaseContextDataset( result .rows ?? null, result.rowCount ?? null);

const result2log = (result)=>({
    rows      :  result.rows,
    count     :  result.count,
});

async function __query( sql, numberedParams, namedParams ) {
  try {
    if ( ! this.is_connected() )
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


/*
 * The template literal ${} placeholder disallows any other way than statically resolving the variable.
 * As a result, it is necessary to expand nargs passed as JSON from the outside as a variable.
 * Originally, the design was based on the assumption that this nargs would be automatically validated using vanilla-schema-validator.
 * In other words, using the ${} placeholder will overturn the previous design policy.
 * The design policy was to reduce the amount of coding by automating the validation process.
 *
 * (Thu, 16 May 2024 17:44:44 +0900)
 */


/*async*/ function sql( strings, ...values ) {
  const sqlfunc = sqlmacro(strings, ...values );
  return (nargs)=>{
    return this.query( sqlfunc(nargs), nargs);
  };
}
DatabaseContext.prototype.sql = sql;


function is_connected() {
  // >>> MODIFIED (Mon, 05 Jun 2023 15:26:11 +0900)
  // return this.__pgClient != null;
  this.__ensure_client();
  return this.__connected === true;
  // <<< MODIFIED (Mon, 05 Jun 2023 15:26:11 +0900)
}
DatabaseContext.prototype.is_connected = is_connected;

async function connect_database() {
  this.logger.output({
    type   : 'connect-database',
  });
  if ( this.is_connected() )
    throw new DatabaseContextError({message:'this context has already established a connection.'});

  // >>> MODIFIED (Mon, 05 Jun 2023 15:26:11 +0900)
  // this.__pgClient = new Client();
  // <<< MODIFIED (Mon, 05 Jun 2023 15:26:11 +0900)
  this.__pgClient.connect();
  this.__connected = true;

  // this.__pgClient = await pool.connect();

  return this;
};
DatabaseContext.prototype.connect_database = connect_database;

async function disconnect_database() {
  this.logger.output({
    type   : 'disconnect-database',
  });
  if ( this.is_connected() ) {
    if ( 'end' in this.__pgClient ) {
      await this.__pgClient.end();
    } else if ( 'release' in this.__pgClient ) {
      await this.__pgClient.release();
    } else {
      console.error('__pgClient has not method to finalize');
    }
    this.__connected = false;
    this.__pgClient = null;
  }
  return this;
};
DatabaseContext.prototype.disconnect_database = disconnect_database;

async function initializeContextOfDatabaseContext() {
  /*
   * Q: Why don't you refer the `autoCommit` field directly? It seems that it
   * is not necessary to copy the value to another field.
   * A: Because bad behaving users could modify the value while they are still
   *    in their session. If autoCommit is true before starting a session and it
   *    is modified afterwards, the connection state would not properly
   *    maintained. Therefore, a snapshot needs to be created when it starts a
   *    new session.
   *
   * Wed, 17 Jan 2024 13:36:24 +0900
   */
  // console.log( 'this.getOptions().autoCommit', this.getOptions().autoCommit );

  /*
   * Note that `autoCommit` option implies `autoConnect` option.
   *  Wed, 17 Jan 2024 13:40:07 +0900
   */
  if ( this.getOptions().autoConnect === true || this.getOptions().autoCommit === true ) {
    this.__autoConnect = true;
    await this.connect_database();
  }

  if ( this.getOptions().autoCommit === true ) {
    // console.log( 'autoCommit is true ' );
    this.__autoCommit = true;
    await this.begin_transaction();
  };
}
DatabaseContext.prototype.initializeContextOfDatabaseContext = initializeContextOfDatabaseContext;

async function finalizeContextOfDatabaseContext(is_successful) {
  const context = this;
  if ( context.is_connected() ) {

    // console.log( 'context.__autoCommit', context.__autoCommit );

    if ( context.__autoCommit === true ) {
      try {
        if ( is_successful ) {
          context.logger.log( 'commit for finalization' );
          await context.commit_transaction();
        } else {
          context.logger.log( 'rollback for finalization' );
          await context.rollback_transaction();
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

  /*
   * Note that `autoCommit` option implies `autoConnect` option.
   * Wed, 17 Jan 2024 13:40:07 +0900
   */
    if( context.__autoConnect || context.__autoCommit ) {
      // This is to ensures that the current connection is properly finalized.
      try {
        context.logger.log( 'disconnect_database for finalization' );
        if ( context.is_connected() ) {
          await context.disconnect_database( context );
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
export { shutdownDatabaseContext };
// <<<


async function begin_transaction() {
  try {
    this.logger.enter( 'begin_transaction' );
    return await this.query( SQL_BEGIN );
  } finally {
    this.logger.leave( 'begin_transaction' );
  }
}
DatabaseContext.prototype.begin_transaction = begin_transaction;

async function commit_transaction() {
  try {
    this.logger.enter( 'commit_transaction' );
    return await this.query( SQL_COMMIT );
  } finally {
    this.logger.leave( 'commit_transaction' );
  }
  this.trace( 'commit_transaction' );
}
DatabaseContext.prototype.commit_transaction = commit_transaction;

async function rollback_transaction() {
  try {
    this.logger.enter( 'rollback_transaction' );
    return await this.query( SQL_ROLLBACK );
  } finally {
    this.logger.leave( 'rollback_transaction' );
  }
}
DatabaseContext.prototype.rollback_transaction = rollback_transaction;



/* ===================================================================
 *
 * TAG_EVENTS
 *
 * ===================================================================
 */
DatabaseContext.defineMethod( async function register_pg_eventhandler( event_id, fn ) {
  this.__ensure_client();
  try {
    this.logger.enter( 'register_pg_eventhandler' );
    this.__pgClient.on( event_id, async(...args)=>{
      await fn.apply( this, args );
    });
  } finally {
    this.logger.leave( 'register_pg_eventhandler' );
  }
},{
});



