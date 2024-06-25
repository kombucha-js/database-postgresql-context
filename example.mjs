
import { execute } from "database-postgresql-context/utils.mjs";

await execute( async function proc() {
  console.log('hello')
  let is_successful = false;
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

    is_successful = true;

  } finally {
    await this.disconnect_database();
    this.logger.reportResult( is_successful );
  }
});

