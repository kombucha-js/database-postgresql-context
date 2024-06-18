
import { mixin }              from 'mixin-prototypes' ;
import { AsyncContext }       from 'asynchronous-context' ;
import { DatabaseContext }    from 'database-postgresql-context' ;
import { dotenvFromSettings } from "asynchronous-context/env" ;
dotenvFromSettings();

class Hello  {
  ctor(...args) {
    this.contextInitializers.push( async function databaseContextInitializer() {
      await this.register_pg_eventhandler( 'notice', async (msg)=>{
        const __msg = { ...msg };
        for ( const i in __msg ) {
          if ( typeof __msg[i] === 'undefined' ) {
            delete __msg[i];
          }
        }
        this.logger.output( {
          type : 'postgresql-notice',
          contents:__msg,
        });
      });
    });

    this.contextFinalizers.push(async function databaseContextFinalizer(is_successful) {
    });
  }
}

await (
  (async ()=>{
    const THello = mixin( 'THello', AsyncContext, DatabaseContext , Hello  );
    const context = THello.create({  autoConnect: false, autoCommit:false, coloredReport:true, reportMethod:'stderr' });
    await context.executeTransaction( async function() {
      let succeeded = false;
      try {
        await this.connect_database();
        await this.begin_transaction();
        await this.sql`
         DO
         $SQL$
           BEGIN
             RAISE NOTICE 'hi!!! this is an intentional error!!!';
           END;
         $SQL$
       `();
        await this.commit_transaction();

        succeeded = true;

      } finally {
        await this.disconnect_database();
        this.logger.reportResult( succeeded );
      }
    });
  })()
)


