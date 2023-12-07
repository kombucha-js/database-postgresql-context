
import { DatabaseContext } from  'database-postgresql-context' ;
import { shutdownDatabaseContext } from  'database-postgresql-context' ;

class TestContext extends DatabaseContext {
  constructor (...args) {
    super(...args);
  }
}
function createContext(...args) {
  return new TestContext( ...args );
}

import assert from  'node:assert/strict' ;
import { test, describe, it, before, after }  from  'node:test' ;
import { createTest } from  'asynchronous-context/test-utils' ;
import { randomcat } from  'randomcat' ;

const testdb = createTest( it, createContext, { suppressSuccessfulReport:true, autoCommit:false, showReport:true, coloredReport:true, reportMethod:'stderr' } );

const IN_USERS = [
  { username:'hello_world', local_attrs: {age:12, nickname: 'foo-12',  nickname1: 'bar-12',  nickname2: 'bum-12',} },
  { username:'hello'      , local_attrs: {age:13, nickname: 'foo-13',  nickname1: 'bar-13',  nickname2: 'bum-13',} },
  { username:'foo'        , local_attrs: {age:14, nickname: 'foo-14',  nickname1: 'bar-14',  nickname2: 'bum-14',} },
  { username:'bar'        , local_attrs: {age:15, nickname: 'foo-15',  nickname1: 'bar-15',  nickname2: 'bum-15',} },
  { username:'HELLO'      , local_attrs: {age:16, nickname: 'foo-16',  nickname1: 'bar-16',  nickname2: 'bum-16',} },
  { username:'FOO'        , local_attrs: {age:17, nickname: 'foo-17',  nickname1: 'bar-17',  nickname2: 'bum-17',} },
  { username:'BAR'        , local_attrs: {age:18, nickname: 'foo-18',  nickname1: 'bar-18',  nickname2: 'bum-18',} },
];

{
  const lst = [
    'nickname',
    'section',
    'cat',
    'comment',
    'house',
    'country',
    'district',
    'foo',
    'bar',
    'beta-nickname',
    'beta-section',
    'beta-cat',
    'beta-comment',
    'beta-house',
    'beta-country',
    'beta-district',
    'beta-foo',
    'beta-bar',
  ];
  for ( const id of lst ) {
    let c = 0;
    for ( let e of IN_USERS ) {
      e.local_attrs[id] = randomcat() + '-' + c + '-' + id;
      c++;
    }
  }
}

// const OUT_USERS = [];
globalThis.OUT_USERS = [];


describe( 'test database-postgresql-context', ()=>{
  after(async() => {
    console.log( "shutdownDatabaseContext() done" );
    (await shutdownDatabaseContext());
  });

  testdb( 'first test', async function() {
    const connection = this;
    Object.entries(connection).forEach( ([key,value])=>connection.logger.log({key,value}));
    connection.logger.warn( 'hello' );
  });

  testdb( 'second test', async function() {
    const connection = this;
    await this.connect_database();
    await this.begin_transaction();
    const sql =`
      SELECT *
      FROM (
        VALUES
        (0::int, 0::int,0::int, 'null'::varchar),
        (0,0,1,'0,0,1'),
        (0,0,2,'0,0,2'),
        (0,0,3,'0,0,3'),
        (0,0,4,'0,0,4'),
        (0,0,5,'0,0,5'),
        (0,0,6,'0,0,6'),
        (0,0,7,'0,0,7'),
        (0,0,8,'0,0,8'),
        (0,1,1,'0,1,1'),
        (0,1,2,'0,1,2'),
        (0,1,3,'0,1,3'),
        (0,1,4,'0,1,4'),
        (0,1,5,'0,1,5'),
        (0,1,6,'0,1,6'),
        (0,1,7,'0,1,7'),
        (0,1,8,'0,1,8'),
        (0,2,1,'0,2,1'),
        (0,2,2,'0,2,2'),
        (0,2,3,'0,2,3'),
        (0,2,4,'0,2,4'),
        (0,2,5,'0,2,5'),
        (0,2,6,'0,2,6'),
        (0,2,7,'0,2,7'),
        (0,2,8,'0,2,8'),
        (1,0,1,'1,0,1'),
        (1,0,2,'1,0,2'),
        (1,0,3,'1,0,3'),
        (1,0,4,'1,0,4'),
        (1,0,5,'1,0,5'),
        (1,0,6,'1,0,6'),
        (1,0,7,'1,0,7'),
        (1,0,8,'1,0,8'),
        (1,1,1,'1,1,1'),
        (1,1,2,'1,1,2'),
        (1,1,3,'1,1,3'),
        (1,1,4,'1,1,4'),
        (1,1,5,'1,1,5'),
        (1,1,6,'1,1,6'),
        (1,1,7,'1,1,7'),
        (1,1,8,'1,1,8'),
        (1,2,1,'1,2,1'),
        (1,2,2,'1,2,2'),
        (1,2,3,'1,2,3'),
        (1,2,4,'1,2,4'),
        (1,2,5,'1,2,5'),
        (1,2,6,'1,2,6'),
        (1,2,7,'1,2,7'),
        (1,2,8,'1,2,8')
      ) AS tbl ( foo_id , bar_id, buz_id, foo_value )
      WHERE
        foo_id = $foo_id AND
        bar_id = $bar_id AND
        buz_id = $foo_id
    `;

    // connection.logger.error( connection.query( sql ) );
    const res = await connection.query( sql, {foo_id:1,bar_id:2});
    assert.deepEqual( res.firstRow, {foo_id:1, bar_id:2, buz_id:1, foo_value:'1,2,1' } ) ;

    await this.commit_transaction();
    await this.disconnect_database();
  });

  testdb( 'third test', async function() {
    const connection = this;
    await this.connect_database();
    await this.begin_transaction();
    const sql =`
      SELECT *
      FROM (
        VALUES
        (0::int, 0::int,0::int, 'null'::varchar),
        (0,0,1,'0,0,1'),
        (0,0,2,'0,0,2'),
        (0,0,3,'0,0,3'),
        (0,0,4,'0,0,4'),
        (0,0,5,'0,0,5'),
        (0,0,6,'0,0,6'),
        (0,0,7,'0,0,7'),
        (0,0,8,'0,0,8'),
        (0,1,1,'0,1,1'),
        (0,1,2,'0,1,2'),
        (0,1,3,'0,1,3'),
        (0,1,4,'0,1,4'),
        (0,1,5,'0,1,5'),
        (0,1,6,'0,1,6'),
        (0,1,7,'0,1,7'),
        (0,1,8,'0,1,8'),
        (0,2,1,'0,2,1'),
        (0,2,2,'0,2,2'),
        (0,2,3,'0,2,3'),
        (0,2,4,'0,2,4'),
        (0,2,5,'0,2,5'),
        (0,2,6,'0,2,6'),
        (0,2,7,'0,2,7'),
        (0,2,8,'0,2,8'),
        (1,0,1,'1,0,1'),
        (1,0,2,'1,0,2'),
        (1,0,3,'1,0,3'),
        (1,0,4,'1,0,4'),
        (1,0,5,'1,0,5'),
        (1,0,6,'1,0,6'),
        (1,0,7,'1,0,7'),
        (1,0,8,'1,0,8'),
        (1,1,1,'1,1,1'),
        (1,1,2,'1,1,2'),
        (1,1,3,'1,1,3'),
        (1,1,4,'1,1,4'),
        (1,1,5,'1,1,5'),
        (1,1,6,'1,1,6'),
        (1,1,7,'1,1,7'),
        (1,1,8,'1,1,8'),
        (1,2,1,'1,2,1'),
        (1,2,2,'1,2,2'),
        (1,2,3,'1,2,3'),
        (1,2,4,'1,2,4'),
        (1,2,5,'1,2,5'),
        (1,2,6,'1,2,6'),
        (1,2,7,'1,2,7'),
        (1,2,8,'1,2,8')
      ) AS tbl ( foo_id , bar_id, buz_id, foo_value )
      WHERE
        foo_id = $foo_id AND
        bar_id = $bar_id AND
        buz_id = $buz_id
    `;

    // connection.logger.error( connection.query( sql ) );
    const res = await connection.query( sql, {foo_id:0,bar_id:1,buz_id:7});
    assert.deepEqual(res.firstRow,  {foo_id:0, bar_id:1, buz_id:7, foo_value:'0,1,7' } ) ;

    // no
    await this.commit_transaction();
    await this.disconnect_database();
  });

});
