
class DatabaseContextError extends Error {
  constructor( messageObject, options, ...args) {
    super( messageObject.message, options, ...args );
    this.messageObject = messageObject || {};
  }
}
export { DatabaseContextError };

class DatabaseContextDataset {
  constructor( rows, count ) {
    if ( rows !== null && ! Array.isArray( rows ) ) {
      throw new DatabaseContextError({message:'the `rows` argument should be either null or an array'});
    }
    if ( count !== null && typeof count !== 'number' ) {
      throw new DatabaseContextError({message:'the `rows` argument should be either null or an array'});
    }
    this.__rows  = rows;
    this.__count = count;
  }
  get row_count() {
    if ( typeof this.__count  === 'number' ) {
      return this.__count;
    } else {
      throw new DatabaseContextError({message:'the query was not an update query'});
    }
  }
  get all_rows() {
    return this.__rows;
  }
  get first_row() {
    const row = this.first_row_or_null;
    if ( row === null ) {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
    return row;
  }
  get first_row_or_null() {
    if ( Array.isArray( this.__rows ) && 0 < this.__rows.length ) {
      return this.__rows[0];
    } else {
      return null;
    }
  }
  get single_row() {
    if ( Array.isArray( this.__rows ) ) {
      if ( this.__rows.length < 1 ) {
        throw new DatabaseContextError({message:'the result has no dataset'});
      } else if ( this.__rows.length === 1 ) {
        return this.__rows[0];
      } else {
        throw new DatabaseContextError({message:`NOT UNIQUE : the result has more than one rows ${this.__rows.length} `});
      }
    } else {
      throw new DatabaseContextError({message:'the result has no dataset'});
    }
  }
  get single_row_or_null() {
    if ( Array.isArray( this.__rows ) ) {
      if ( this.__rows.length < 1 ) {
        return null;
      } else if ( this.__rows.length === 1 ) {
        return this.__rows[0];
      } else {
        throw new DatabaseContextError({message:`NOT UNIQUE : the result has more than one rows ${this.__rows.length} `});
      }
    } else {
      return null;
    }
  }
  get result_array() {
    throw new DatabaseContextError( MSG_SINGLE_RESULTSET_ERROR );
  }

  //

  get count() {
    return this.row_count;
  }
  get rows() {
    return this.all_rows;
  }
  get firstRow() {
    return this.first_row;
  }
  get firstRowOrNull() {
    return this.first_row_or_null;
  }
  get singleRow() {
    return this.single_row;
  }
  get singleRowOrNull() {
    return this.single_row_or_null;
  }
  get resultArray() {
    return this.result_array;
  }
}
export { DatabaseContextDataset };


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
  get row_count() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get all_rows() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get first_row() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get first_row_or_null() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get single_row() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get single_row_or_null() {
    throw new DatabaseContextError( MSG_MULTIPLE_RESULTSET_ERROR );
  }
  get result_array() {
    return [ ...this.#results ];
  }

  //
  get count() {
    return this.row_count;
  }
  get rows() {
    return this.all_rows;
  }
  get firstRow() {
    return this.first_row;
  }
  get firstRowOrNull() {
    return this.first_row_or_null;
  }
  get singleRow() {
    return this.single_row;
  }
  get singleRowOrNull() {
    return this.single_row_or_null;
  }
  get resultArray() {
    return this.result_array;
  }
}
export { DatabaseContextMultipleDataset };


