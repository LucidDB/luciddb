var start = 0;
var step = 1;
var meta_data = inputSet.getMetaData(); // inputSet is global
var columns = meta_data.getColumnCount();
var names = [];
var counter = start - step;
while (inputSet.next()) {
  var i = 1;
  for (; i < columns; ++i) {
    // resultInserter is global
    resultInserter.setObject(i, inputSet.getObject(i));
    if (names.length < columns) {
      names.push(meta_data.getColumnName(i));
    }
  }

  counter += step;
  resultInserter.setLong(i, counter);
  resultInserter.executeUpdate();
}
