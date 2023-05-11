
class DatabaseContextError extends Error {
  constructor( messageObject, options, ...args) {
    super( messageObject.message, options, ...args );
    this.messageObject = messageObject || {};
  }
}
module.exports.DatabaseContextError = DatabaseContextError;

class DatabaseContextDataset {
  #rows  = null;
  #count = null;
  constructor( rows, count ) {
    if ( rows !== null && ! Array.isArray( rows ) ) {
      throw new DatabaseContextError({message:'the `rows` argument should be either null or an array'});
    }
    if ( count !== null && typeof count !== 'number' ) {
      throw new DatabaseContextError({message:'the `rows` argument should be either null or an array'});
    }
    this.#rows  = rows;
    this.#count = count;
  }
  get count() {
    if ( typeof this.#count  === 'number' ) {
      return this.#count;
    } else {
      throw new DatabaseContextError({message:'the query was not an update query'});
    }
  }
  get rows() {
    return this.#rows;
  }
  get firstRow() {
    const row = this.firstRowOrNull();
    if ( row === null ) {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
    return row;
  }
  get firstRowOrNull() {
    if ( Array.isArray( this.#rows ) && 0 < this.#rows.length ) {
      return this.#rows[0];
    } else {
      return null;
    }
  }
  get singleRow() {
    if ( Array.isArray( this.#rows ) ) {
      if ( this.#rows.length < 1 ) {
        throw new DatabaseContextError({message:'the result has no dataset'});
      } else if ( this.#rows.length === 1 ) {
        return this.#rows[0];
      } else {
        throw new DatabaseContextError({message:`NOT UNIQUE : the result has more than one rows ${this.#rows.length} `});
      }
    } else {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
  }
  get singleRowOrNull() {
    if ( Array.isArray( this.#rows ) ) {
      if ( this.#rows.length < 1 ) {
        return null;
      } else if ( this.#rows.length === 1 ) {
        return this.#rows[0];
      } else {
        throw new DatabaseContextError({message:`NOT UNIQUE : the result has more than one rows ${this.#rows.length} `});
      }
    } else {
      return null;
    }
  }
  get resultArray() {
    throw new DatabaseContextError( MSG_SINGLE_RESULTSET_ERROR );
  }
}
module.exports.DatabaseContextDataset = DatabaseContextDataset;


const MSG_SINGLE_RESULTSET_ERROR   = 'single resultset error / cannot call a method for multiple resultsets. ';
const MSG_MULTIPLE_RESULTSET_ERROR = 'multiple resultset error / cannot call a method for single resultset. ( maybe you accidentally get multiple results )' ;
class DatabaseContextMultipleDataset {
  #results = null;
  constructor( results ) {
    if ( ! Array.isArray( results ) ) {
      throw new DatabaseContextError( 'the specified results object is not an array' );
    }

    this.#results = results;
  }
  get count() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get rows() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get firstRow() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get firstRowOrNull() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get singleRow() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get singleRowOrNull() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get resultArray() {
    return [ ...this.#results ];
  }
}
module.exports.DatabaseContextMultipleDataset = DatabaseContextMultipleDataset;


