

const plainsql = (strings,...values)=>{
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
};
module.exports = plainsql;
