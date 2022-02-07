package main

import (
  "fmt"
  "os"
  "bufio"
  "io"
  "time"
  "strings"
  "strconv"
  "math"
  "math/cmplx"
  "runtime"
  // packages for db
  "database/sql"
  _ "github.com/lib/pq"
)

const (
  // const vars for db
  host              = "127.0.0.1"
  port              = 5432
  user              = "XXXX-1"     // username
  password          = "your-password" // PostgreSQL password
  dbname            = "climatedb"
  tablename         = "pairsbwr"
  tablenamedft      = "pairsbwrdft"
  pairsbwrschema    = "id INT UNIQUE NOT NULL, pair VARCHAR(30) UNIQUE NOT NULL, meanx VARCHAR(10000), meany VARCHAR(10000), sigmax VARCHAR(10000), sigmay VARCHAR(10000), cxy VARCHAR(10000)"
  pairsbwrheader    = "(id, pair, meanx, meany, sigmax, sigmay, cxy)"
  pairsbwrdftschema = "id INT UNIQUE NOT NULL, pair VARCHAR(30) UNIQUE NOT NULL, meanx VARCHAR(10000), meany VARCHAR(10000), sigmax VARCHAR(10000), sigmay VARCHAR(10000), dxy VARCHAR(10000)"
  pairsbwrdftheader = "(id, pair, meanx, meany, sigmax, sigmay, dxy)"
)

type Pair struct {
  leftLocation int    // location of left stream
  rightLocation int   // location of right stream
  indexOfRow int      // row index in matrix
  indexOfCol int      // column index in matrix
}

/* Struct for data point */
type Point struct {
  timestamp int
  latitude int
  longitude int
  location int
  temperature float64
}

/* Struct to store basic window statistics */
type BasicWindowResult struct {
  pair Pair
  slicesOfMeanX *([]float64)
  slicesOfMeanY *([]float64)
  slicesOfSigmaX *([]float64)
  slicesOfSigmaY *([]float64)
  slicesOfCXY *([]float64)
}

/* Struct to store basic window dft statistics */
type BasicWindowDFTResult struct {
  pair Pair
  slicesOfMeanX *([]float64)
  slicesOfMeanY *([]float64)
  slicesOfSigmaX *([]float64)
  slicesOfSigmaY *([]float64)
  slicesOfDXY *([]float64)
  // For updates
  slicesOfSumSquaredX *([]float64)
  slicesOfSumSquaredY *([]float64)
}

/* Struct for insertion to db, unique to each other */
type SerializedPair struct {
  value string
}

/* Serialized BasicWindowResult */
type RowBWR struct {
  pair SerializedPair // leftLocation,rightLocation,indexOfRow,indexOfCol
  meanX string        // mean_x_1,mean_x_2,mean_x_3...
  meanY string        // mean_y_1,mean_y_2,mean_y_3...
  sigmaX string       // sigma_x_1,sigma_x_2,sigma_x_3...
  sigmaY string       // sigma_y_1,sigma_y_2,sigma_y_3...
  cXY string          // cxy_1,cxy_2,cxy_3...
}

/* Serialized BasicWindowDFTResult */
type RowBWRDFT struct {
  pair SerializedPair // leftLocation,rightLocation,indexOfRow,indexOfCol
  meanX string        // mean_x_1,mean_x_2,mean_x_3...
  meanY string        // mean_y_1,mean_y_2,mean_y_3...
  sigmaX string       // sigma_x_1,sigma_x_2,sigma_x_3...
  sigmaY string       // sigma_y_1,sigma_y_2,sigma_y_3...
  dXY string          // dxy_1,dxy_2,dxy_3...
}

/* Data stored in channel */
type DataOfChannel struct {
  statement string
}

/* --- Functions related to database operations --- */
/* Exec handler */
func execDB(db *sql.DB, sqlStatementPtr *string) {
  _, err := db.Exec(*sqlStatementPtr)
  if err != nil {
    panic(err)
  }
}

/* Open db */
func openDB(dbNamePtr *string) *sql.DB {
  var psqlInfo string
  if (dbNamePtr == nil) {
    psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s sslmode=disable",
    host, port, user, password)
  } else {
    psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host, port, user, password, *dbNamePtr)
  }
  // Open a connection, 1st arg: server name, 2nd arg: connection string
  db, err := sql.Open("postgres", psqlInfo)
  if err != nil {
    panic(err)
  }
  // Check whether or not the connection string was 100% correct
  err = db.Ping()
  if err != nil {
    panic(err)
  }
  fmt.Println("Successfully connected!")
  return db
}

/* Close db */
func closeDB(db *sql.DB) {
  db.Close()
}

/* Get size of db */
func getSizeOfDB(dbName string) int {
  // Get db size
  db := openDB(&dbName)
  st := fmt.Sprintf("select pg_database_size('%s');", dbName)
  rows, err := db.Query(st)
  if err != nil {
    panic(err)
  }
  var sizeStr string
  for rows.Next() {
    rows.Scan(&sizeStr)
    fmt.Println("db size: ", sizeStr)
  }
  closeDB(db) // Close the database
  intVal, _ := strconv.Atoi(sizeStr)
  return intVal
}

/* Create a new database in postgreSQL */
func createNewDB(dbName string) {
  db := openDB(nil)
  // Create a new table
  sqlStatement := "CREATE DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("DATABASE CREATED: ", dbName)
  closeDB(db)
  fmt.Println("Closed!")
}

/* Delete the database (dbname) when it is closed */
func deleteDB(dbName string) {
  db := openDB(nil)
  defer closeDB(db)
  // Delete the table
  sqlStatement := "DROP DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("DATABASE DELETED: ", dbName)
  fmt.Println("Successfully deleted " + dbName + "!")
}

/* Create a table with schema in the specific database */
func createTable(db *sql.DB, tableName string, schema string) {
  sqlStatement := fmt.Sprintf("CREATE TABLE %s (%s);", tableName, schema)
  execDB(db, &sqlStatement)
  fmt.Println("TABLE CREATED: ", tableName)
}

/* Delete a table in the database */
func deleteTable(db *sql.DB, tableName string) {
  sqlStatement := "DROP TABLE " + tableName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("TABLE DELETED: ", tableName)
}

/* Insert one row (basic window result) to db */
func insertRowBWR(db *sql.DB, bwr *BasicWindowResult, id int, tableName string) {
  rowBWR := RowBWR{SerializedPair{""}, "", "", "", "", ""}
  serializeBWR(bwr, &rowBWR)
  sqlStatement := fmt.Sprintf("INSERT INTO %s %s VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');", 
  tableName, pairsbwrheader, id, rowBWR.pair.value, rowBWR.meanX, rowBWR.meanY, rowBWR.sigmaX, rowBWR.sigmaY, rowBWR.cXY)
  execDB(db, &sqlStatement)
}

/* Insert one row (basic window result + dft) to db */
func insertRowBWRDFT(db *sql.DB, bwrdft *BasicWindowDFTResult, id int, tableName string) {
  rowBWRDFT := RowBWRDFT{SerializedPair{""}, "", "", "", "", ""}
  serializeBWRDFT(bwrdft, &rowBWRDFT)
  sqlStatement := fmt.Sprintf("INSERT INTO %s %s VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');", 
  tableName, pairsbwrdftheader, id, rowBWRDFT.pair.value, rowBWRDFT.meanX, rowBWRDFT.meanY, rowBWRDFT.sigmaX, rowBWRDFT.sigmaY, rowBWRDFT.dXY)
  execDB(db, &sqlStatement)
}

/* Append row statistics to rows statement */
func appendRowBWR(statement *strings.Builder, bwr *BasicWindowResult, id int) {
  rowBWR := RowBWR{SerializedPair{""}, "", "", "", "", ""}
  serializeBWR(bwr, &rowBWR)
  (*statement).WriteString(fmt.Sprintf(" (%d, '%s', '%s', '%s', '%s', '%s', '%s')",
  id, rowBWR.pair.value, rowBWR.meanX, rowBWR.meanY, rowBWR.sigmaX, rowBWR.sigmaY, rowBWR.cXY))
}

/* Append row statistics to rows statement (with dft) */
func appendRowBWRDFT(statement *strings.Builder, bwrdft *BasicWindowDFTResult, id int) {
  rowBWRDFT := RowBWRDFT{SerializedPair{""}, "", "", "", "", ""}
  serializeBWRDFT(bwrdft, &rowBWRDFT)
  (*statement).WriteString(fmt.Sprintf(" (%d, '%s', '%s', '%s', '%s', '%s', '%s')",
  id, rowBWRDFT.pair.value, rowBWRDFT.meanX, rowBWRDFT.meanY, rowBWRDFT.sigmaX, rowBWRDFT.sigmaY, rowBWRDFT.dXY))
}

/* Insert rows to db in strings.Builder */
func insertRowsBWR(db *sql.DB, statement *strings.Builder) {
  str := (*statement).String()
  execDB(db, &str)
}

/* Insert rows to db in string */
func insertRowsBWRString(db *sql.DB, s *string) {
  execDB(db, s)
}

/* Helper function: transfer slices of float64 to a row of string */
func slicesToString(slices *([]float64), row *string) {
  var sb strings.Builder
  for i := 0; i < len(*slices); i += 1 {
    sb.WriteString(fmt.Sprintf("%.5f", (*slices)[i]))
    if (i != len(*slices) - 1) {
      sb.WriteString(",")
    }
  }
  *row = sb.String()
  if len(*row) > 10000 {
    panic("Size of string is too large")
  }
}

/* Serialize BasicWindowResult to RowBWR in case for insertion */
func serializeBWR(bwr *BasicWindowResult, rowBWR *RowBWR) {
  // Serialize Pair
  serializedPairString := fmt.Sprintf("%d,%d,%d,%d", 
    bwr.pair.leftLocation, bwr.pair.rightLocation, bwr.pair.indexOfRow, bwr.pair.indexOfCol)
  if len(serializedPairString) > 30 {
    fmt.Println("len(serializedPairString): ", len(serializedPairString))
  }
  serializedPair := SerializedPair{serializedPairString}
  rowBWR.pair = serializedPair
  slicesToString(bwr.slicesOfMeanX, &rowBWR.meanX)
  slicesToString(bwr.slicesOfMeanY, &rowBWR.meanY)
  slicesToString(bwr.slicesOfSigmaX, &rowBWR.sigmaX)
  slicesToString(bwr.slicesOfSigmaY, &rowBWR.sigmaY)
  slicesToString(bwr.slicesOfCXY, &rowBWR.cXY)
}

/* Serialize BasicWindowDFTResult to RowBWRDFT in case for insertion */
func serializeBWRDFT(bwrdft *BasicWindowDFTResult, rowBWRDFT *RowBWRDFT) {
  // Serialize Pair
  serializedPairString := fmt.Sprintf("%d,%d,%d,%d", 
    bwrdft.pair.leftLocation, bwrdft.pair.rightLocation, bwrdft.pair.indexOfRow, bwrdft.pair.indexOfCol)
  if len(serializedPairString) > 30 {
    fmt.Println("len(serializedPairString): ", len(serializedPairString))
  }
  serializedPair := SerializedPair{serializedPairString}
  rowBWRDFT.pair = serializedPair
  slicesToString(bwrdft.slicesOfMeanX, &rowBWRDFT.meanX)
  slicesToString(bwrdft.slicesOfMeanY, &rowBWRDFT.meanY)
  slicesToString(bwrdft.slicesOfSigmaX, &rowBWRDFT.sigmaX)
  slicesToString(bwrdft.slicesOfSigmaY, &rowBWRDFT.sigmaY)
  slicesToString(bwrdft.slicesOfDXY, &rowBWRDFT.dXY)
}

/* Helper function: transfer a row of string to slices of float64 (index is from start to end - 1) */
func stringToSlices(row *string, slices *([]float64), start int, end int) {
  strSlices := strings.Split(*row, ",")
  for index, str := range strSlices {
    if index < start {
      continue
    }
    if index >= end {
      break
    }
    floatVal, err := strconv.ParseFloat(str, 64)
    if err != nil {
      panic(err)
    }
    (*slices)[index - start] = floatVal
  }
}

/* Serialize RowBWR to BasicWindowResult */
func deserializRowBWR(rowBWR *RowBWR, bwr *BasicWindowResult, start int, end int) {
  _, err := fmt.Sscanf(rowBWR.pair.value, "%d,%d,%d,%d", &bwr.pair.leftLocation, &bwr.pair.rightLocation, &bwr.pair.indexOfRow, &bwr.pair.indexOfCol)
  if err != nil {
    panic(err)
  }
  stringToSlices(&rowBWR.meanX, bwr.slicesOfMeanX, start, end)
  stringToSlices(&rowBWR.meanY, bwr.slicesOfMeanY, start, end)
  stringToSlices(&rowBWR.sigmaX, bwr.slicesOfSigmaX, start, end)
  stringToSlices(&rowBWR.sigmaY, bwr.slicesOfSigmaY, start, end)
  stringToSlices(&rowBWR.cXY, bwr.slicesOfCXY, start, end)
}

/* Serialize RowBWRDFT to BasicWindowDFTResult */
func deserializRowBWRDFT(rowBWRDFT *RowBWRDFT, bwrdft *BasicWindowDFTResult, start int, end int) {
  _, err := fmt.Sscanf(rowBWRDFT.pair.value, "%d,%d,%d,%d", &bwrdft.pair.leftLocation, &bwrdft.pair.rightLocation, &bwrdft.pair.indexOfRow, &bwrdft.pair.indexOfCol)
  if err != nil {
    panic(err)
  }
  stringToSlices(&rowBWRDFT.meanX, bwrdft.slicesOfMeanX, start, end)
  stringToSlices(&rowBWRDFT.meanY, bwrdft.slicesOfMeanY, start, end)
  stringToSlices(&rowBWRDFT.sigmaX, bwrdft.slicesOfSigmaX, start, end)
  stringToSlices(&rowBWRDFT.sigmaY, bwrdft.slicesOfSigmaY, start, end)
  stringToSlices(&rowBWRDFT.dXY, bwrdft.slicesOfDXY, start, end)
}

/* Helper function: update matrix */
func updateMatrix(matrix *([][]int), thres float64, pair *Pair, slicesOfMeanX *([]float64), slicesOfMeanY *([]float64), 
  slicesOfSigmaX *([]float64), slicesOfSigmaY *([]float64), slicesOfCXY *([]float64), slicesOfDXY *([]float64), isDFT bool, accurateMatrix *([][]float64)) {
  var corr float64 = 0
  var numerator float64 = 0
  var demoninator1 float64 = 0
  var demoninator2 float64 = 0
  meanXValue := getAvg(slicesOfMeanX)
  meanYValue := getAvg(slicesOfMeanY)
  slicesOfDeltaX := make([]float64, len(*slicesOfMeanX))
  slicesOfDeltaY := make([]float64, len(*slicesOfMeanY))
  size := len(slicesOfDeltaX)
  for i := 0; i < size; i += 1 {
    slicesOfDeltaX[i] = (*slicesOfMeanX)[i] - meanXValue
    slicesOfDeltaY[i] = (*slicesOfMeanY)[i] - meanYValue
  }
  for i := 0; i < size; i += 1 {
    if !isDFT {
      numerator += (*slicesOfSigmaX)[i] * (*slicesOfSigmaY)[i] * (*slicesOfCXY)[i] + slicesOfDeltaX[i] * slicesOfDeltaY[i]
    } else {
      numerator += (*slicesOfSigmaX)[i] * (*slicesOfSigmaY)[i] * (*slicesOfDXY)[i] * (*slicesOfDXY)[i] - 2 * (*slicesOfSigmaX)[i] * (*slicesOfSigmaY)[i] - 2 * slicesOfDeltaX[i] * slicesOfDeltaY[i]
    }
    demoninator1 += (*slicesOfSigmaX)[i] * (*slicesOfSigmaX)[i] + slicesOfDeltaX[i] * slicesOfDeltaX[i]
    demoninator2 += (*slicesOfSigmaY)[i] * (*slicesOfSigmaY)[i] + slicesOfDeltaY[i] * slicesOfDeltaY[i]
  }
  if !isDFT {
    corr = numerator/(math.Sqrt(demoninator1) * math.Sqrt(demoninator2))
  } else {
    var dSquare float64 = 2 + numerator / (math.Sqrt(demoninator1) * math.Sqrt(demoninator2))
    corr = 1 - 0.5 * dSquare
  }
  if accurateMatrix != nil {
    (*accurateMatrix)[pair.indexOfRow][pair.indexOfCol] = corr
    (*accurateMatrix)[pair.indexOfCol][pair.indexOfRow] = corr
  }
  if math.Abs(corr) >= thres {
    (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
    (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
  }
}

/* Helper function: update matrix for DFT incremental method */
func updateMatrixUpdate(matrix *([][]int), thres float64, pair *Pair, slicesOfMeanX *([]float64), slicesOfMeanY *([]float64), 
  slicesOfSigmaX *([]float64), slicesOfSigmaY *([]float64), slicesOfCXY *([]float64), slicesOfDXY *([]float64), slicesOfSumSquaredX *([]float64), slicesOfSumSquaredY *([]float64),
  granularity int, bwrNew *BasicWindowDFTResult, accurateMatrix *([][]float64)) {
  var corr float64 = 0
  meanXValue := getAvg(slicesOfMeanX)
  meanYValue := getAvg(slicesOfMeanY)
  size := len(*slicesOfMeanX)
  slicesOfDeltaX := make([]float64, size)
  slicesOfDeltaY := make([]float64, size)
  for i := 0; i < 1; i += 1 { // Only compute index 0
    slicesOfDeltaX[i] = (*slicesOfMeanX)[i] - meanXValue
    slicesOfDeltaY[i] = (*slicesOfMeanY)[i] - meanYValue
  }

  var stdX, stdY float64
  var sumOfXSquared float64 = getSum(slicesOfSumSquaredX)
  var sumOfYSquared float64 = getSum(slicesOfSumSquaredY)
  var sumOfX float64 = meanXValue * float64(size)
  var sumOfY float64 = meanYValue * float64(size)
  var n float64 = float64(granularity * size)
  stdX = math.Sqrt((sumOfXSquared/n)-((sumOfX*sumOfX)/(n*n)))
  stdY = math.Sqrt((sumOfYSquared/n)-((sumOfY*sumOfY)/(n*n)))

  var alphaX float64 = ((*bwrNew.slicesOfMeanX)[0] - (*slicesOfMeanX)[0]) / float64(size)
  var alphaY float64 = ((*bwrNew.slicesOfMeanY)[0] - (*slicesOfMeanY)[0]) / float64(size)

  var deltaXNew float64 = (*bwrNew.slicesOfMeanX)[0] - meanXValue
  var deltaYNew float64 = (*bwrNew.slicesOfMeanY)[0] - meanYValue

  var dNew float64 = (*bwrNew.slicesOfDXY)[0]
  var cNew float64 = 1 - 0.5 * dNew * dNew

  var A float64 = math.Sqrt(float64(size) * stdX*stdX - (*slicesOfSigmaX)[0]*(*slicesOfSigmaX)[0]) - slicesOfDeltaX[0]*slicesOfDeltaX[0] + (*bwrNew.slicesOfSigmaX)[0]*(*bwrNew.slicesOfSigmaX)[0] - float64(size)*alphaX*alphaX + deltaXNew*deltaXNew
  var B float64 = math.Sqrt(float64(size) * stdY*stdY - (*slicesOfSigmaY)[0]*(*slicesOfSigmaY)[0]) - slicesOfDeltaY[0]*slicesOfDeltaY[0] + (*bwrNew.slicesOfSigmaY)[0]*(*bwrNew.slicesOfSigmaY)[0] - float64(size)*alphaY*alphaY + deltaYNew*deltaYNew
  var oldCorr float64 = (*accurateMatrix)[pair.indexOfRow][pair.indexOfCol]
  corr = (float64(size)*stdX*stdY*oldCorr + (*bwrNew.slicesOfSigmaX)[0]*(*bwrNew.slicesOfSigmaY)[0]*cNew - (*slicesOfSigmaX)[0]*(*slicesOfSigmaY)[0]*(1-0.5*(*slicesOfDXY)[0]*(*slicesOfDXY)[0]) - slicesOfDeltaX[0]*slicesOfDeltaY[0] - float64(size)*alphaX*alphaY + deltaXNew*deltaYNew) / (A*B)

  if math.Abs(corr) >= thres {
    (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
    (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
  }
}

/* Query by the range of ids, updates matrix meanwhile */
func queryRowsDB(db *sql.DB, tableName string, 
  startID int, endID int, matrix *([][]int), thres float64, numberOfBasicwindows int, isDFT bool, 
  queryStart int, queryEnd int) string {
  sqlStatement := fmt.Sprintf("SELECT * FROM %s WHERE id >= %d AND id < %d",
    tableName, startID, endID)
  t0 := time.Now()
  rows, err := db.Query(sqlStatement)
  elapsed := time.Since(t0)
  if err != nil {
    panic(err)
  }
  defer rows.Close()
  lengthOfSlices := queryEnd - queryStart
  if queryEnd < 0 {
    lengthOfSlices = numberOfBasicwindows
    queryStart = 0
    queryEnd = numberOfBasicwindows
  }
  var rowBWR RowBWR
  var rowBWRDFT RowBWRDFT
  for rows.Next() {
    var id int
    var pair string
    var meanX string
    var meanY string
    var sigmaX string
    var sigmaY string
    var cXY string
    var dXY string
    if !isDFT {
      err = rows.Scan(&id, &pair, &meanX, &meanY, &sigmaX, &sigmaY, &cXY)
    } else {
      err = rows.Scan(&id, &pair, &meanX, &meanY, &sigmaX, &sigmaY, &dXY)
    }
    if err != nil {
      panic(err)
    }
    rowBWR = RowBWR{SerializedPair{pair}, meanX, meanY, sigmaX, sigmaY, cXY}
    if isDFT {
      rowBWRDFT = RowBWRDFT{SerializedPair{pair}, meanX, meanY, sigmaX, sigmaY, dXY}
    }
    slicesOfMeanX := make([]float64, lengthOfSlices)
    slicesOfMeanY := make([]float64, lengthOfSlices)
    slicesOfSigmaX := make([]float64, lengthOfSlices)
    slicesOfSigmaY := make([]float64, lengthOfSlices)
    slicesOfCXY := make([]float64, lengthOfSlices)
    slicesOfDXY := make([]float64, lengthOfSlices)
    var bwr BasicWindowResult = BasicWindowResult{Pair{0, 0, 0, 0}, &slicesOfMeanX, &slicesOfMeanY, &slicesOfSigmaX, &slicesOfSigmaY, &slicesOfCXY}
    var bwrdft BasicWindowDFTResult = BasicWindowDFTResult{Pair{0, 0, 0, 0}, &slicesOfMeanX, &slicesOfMeanY, &slicesOfSigmaX, &slicesOfSigmaY, &slicesOfDXY, nil, nil}
    if !isDFT {
      deserializRowBWR(&rowBWR, &bwr, queryStart, queryEnd)
      // Update matrix
      updateMatrix(matrix, thres, &(bwr.pair), bwr.slicesOfMeanX, bwr.slicesOfMeanY, bwr.slicesOfSigmaX, bwr.slicesOfSigmaY, bwr.slicesOfCXY, nil, false, nil)
    } else {
      deserializRowBWRDFT(&rowBWRDFT, &bwrdft, queryStart, queryEnd)
      // Update matrix
      updateMatrix(matrix, thres, &(bwrdft.pair), bwrdft.slicesOfMeanX, bwrdft.slicesOfMeanY, bwrdft.slicesOfSigmaX, bwrdft.slicesOfSigmaY, nil, bwrdft.slicesOfDXY, true, nil)
    }
  }
  return fmt.Sprintf("%v", elapsed)
}

/* String representation of Point */
func displayPoint(dataPoint Point) {
  fmt.Println(fmt.Sprintf("%#v", dataPoint))
}

/* Get average value of a variable-length array */
func getAvg(arr *([]float64)) float64 {
  var sum float64 = 0
  for i := 0; i < len(*arr); i += 1 {
    sum += (*arr)[i]
  }
  return sum/float64(len(*arr))
}

/* Get max value of a variable-length array */
func getMax(arr *([]float64)) float64 {
  var max float64 = (*arr)[0]
  for i := 1; i < len(*arr); i += 1 {
    if (*arr)[i] > max {
      max = (*arr)[i]
    }
  }
  return max
}

/* Get sum of a variable-length array */
func getSum(arr *([]float64)) float64 {
  var sum float64 = 0
  for i := 0; i < len(*arr); i += 1 {
    sum += (*arr)[i]
  }
  return sum
}

/* Transfer an array of string in time format to an array of float64 */
func stringToFloatInSlices(arr []string) ([]float64) {
  res := make([]float64, len(arr))
  for i := 0; i < len(arr); i += 1 {
    res[i] = stringToSeconds(arr[i])
  }
  return res
}

/* Transfer time-formatted string to seconds */
func stringToSeconds(time string) float64 {
  var index int = 0
  var res float64 = 0
  for index < len(time) {
    res += stringToSecondsHelper(time, &index)
  }
  return res
}

/* Helper function for Transfering time-formatted string to seconds */
func stringToSecondsHelper(time string, indexPtr *int) float64 {
  var sb strings.Builder
  var floatVal float64
  for i := *indexPtr; i < len(time); i += 1 {
    s := string(time[i])
    _, err := strconv.Atoi(s)
    if err == nil {
      sb.WriteString(s)
    } else {
      if s == "h" {
        numberStr := sb.String()
        floatVal, _ = strconv.ParseFloat(numberStr, 64)
        *indexPtr = i + 1
        return floatVal * 3600
      }
      if s == "m" {
        if i + 1 >= len(time) {
          floatVal, _ = strconv.ParseFloat(sb.String(), 64)
          *indexPtr = i + 1
          return floatVal * 60
        }
        nextLetter := string(time[i + 1])
        if nextLetter == "s" {
          floatVal, _ = strconv.ParseFloat(sb.String(), 64)
          *indexPtr = i + 2
          return floatVal * 0.001
        }
        floatVal, _ = strconv.ParseFloat(sb.String(), 64)
        *indexPtr = i + 1
        return floatVal * 60
      }
      if s == "µ" {
        floatVal, _ = strconv.ParseFloat(sb.String(), 64)
        *indexPtr = i + 2
        return floatVal * 0.000001
      }
      if s == "s" {
        floatVal, _ = strconv.ParseFloat(sb.String(), 64)
        *indexPtr = i + 1
        return floatVal
      }
      if s == "." {
        sb.WriteString(s)
      }
    }
  }
  return 0
}

/* Transfer a line ([]byte) to Point */
func processLine(line []byte) Point {
  var lineString string = string(line[:])
  strSlices := strings.Split(lineString, ",")
  dataPoint := Point{-1, -1, -1, -1, 0}
  for index, str := range strSlices {
    intVal, intErr := strconv.Atoi(str)
    if (index < 3 && intErr != nil) {
      break
    }
    switch index {
      case 0:
        dataPoint.timestamp = intVal
      case 1:
        dataPoint.latitude = intVal
      case 2:
        dataPoint.longitude = intVal
      case 3:
        str = strings.TrimRight(str, "\n")
        str = strings.TrimRight(str, "\r")
        floatVal, _ := strconv.ParseFloat(str, 64)
        dataPoint.location = dataPoint.longitude + 1000 * dataPoint.latitude
        dataPoint.temperature = floatVal
      default:
        fmt.Println("WARNING: Invalid number of items!")
        break
    }
  }
  return dataPoint
}

/* Arguments: before: set timestamp limit, count: set number of locations limit */
func ReadLine(filePth string, dataMap *(map[int][]Point), 
              before int, count int) error {
  f, err := os.Open(filePth)
  if err != nil {
    return err
  }
  fmt.Println("Open file: SUCCESS")
  //defer f.Close()

  memo := map[int]bool{}

  bfRd := bufio.NewReader(f)
  //i := 0
  for {
    line, err := bfRd.ReadBytes('\n')
    if err != nil {
      if err == io.EOF {
        fmt.Println(len((*dataMap)[1]))
        f.Close()
        return nil
      }
      fmt.Println(err)
      f.Close()
      return err
    }
    dataPoint := processLine(line)
    if dataPoint.timestamp < 0 {
      continue
    }

    if before > 0 && dataPoint.timestamp >= before {
      break
    }

    //
    if count < 0 {
      _, ok := (*dataMap)[dataPoint.location]
      if !ok {
        var points []Point
        points = append(points, dataPoint)
        (*dataMap)[dataPoint.location] = points
      } else {
        (*dataMap)[dataPoint.location] = append((*dataMap)[dataPoint.location], dataPoint)
      }
      continue
    }
    _, ok_memo := memo[dataPoint.location]
    _, ok := (*dataMap)[dataPoint.location]
    if len(memo) < count {
      if ok_memo || ok {
        panic("ERROR")
      }
      var points []Point
      points = append(points, dataPoint)
      (*dataMap)[dataPoint.location] = points
      // Update memo
      memo[dataPoint.location] = true
      //fmt.Println("location: ", dataPoint.location)
    } else {
      if (!ok_memo) {
        continue
      }
      if !ok {
        panic("ERROR!")
      }
      if (ok) {
        (*dataMap)[dataPoint.location] = append((*dataMap)[dataPoint.location], dataPoint)
        //fmt.Println("location_1: ", dataPoint.location)
      }
    }

    //displayPoint(dataPoint) // For debugging
    //i += 1
  }
  f.Close()
  return nil
}

/* Set all items in the mastrix as 0 */
func clearMatrix(matrix *([][]int)) {
  for i := 0; i < len(*matrix); i += 1 {
    for j := 0; j < len((*matrix)[0]); j += 1 {
      (*matrix)[i][j] = 0
    }
  }
}

/* Set all items in the slices as 0 */
func clearSliceOfString(slice *([]string)) {
  for i := 0; i < len(*slice); i += 1 {
    (*slice)[i] = ""
  }
}

/* Get the number of edges with given graph */
func checkMatrix(matrix *([][]int)) int {
  sumOfConnectedPairs := 0
  for i := 0; i < len(*matrix); i += 1 {
    for j := i + 1; j < len((*matrix)[0]); j += 1 {
      if (*matrix)[i][j] == 1 {
        sumOfConnectedPairs += 1
      }
    }
  }
  fmt.Println(sumOfConnectedPairs)
  return sumOfConnectedPairs
}

/* Get locations from given dataMap */
func getLocations(dataMap *(map[int][]Point), locations *([]int)) {
  i := 0
  for key := range *dataMap {
    (*locations)[i] = key
    i += 1
  }
}

// N <= w
func getDFTResult(sigma float64, avg float64, w int, N int, 
    xs *([]float64), result *([]complex128)) {
  for f := 0; f < N; f += 1 {
    var sum complex128 = 0
    for i := 0; i < w; i += 1 {
      xi := ((*xs)[i] - avg) / sigma
      sum += cmplx.Rect(xi, 2 * math.Pi * float64(f * i) / float64(w))
    }
    Xf := complex(1 / math.Sqrt(float64(w)), 0) * sum
    (*result)[f] = Xf
  }
}

/* Get euclidean distance */
func getEuclideanDistance(left *([]complex128), right *([]complex128)) float64 {
  var res float64 = 0
  for i := 0; i < len(*left); i += 1 {
    diff := cmplx.Abs((*left)[i] - (*right)[i])
    res += diff * diff
  }
  return math.Sqrt(res)
}

/* Get the number of basic windows */
func getNumberOfBasicwindows(dataMap *(map[int][]Point), granularity int) int {
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  return len((*dataMap)[locations[0]])/granularity
}

/* Helper function: get bwr from a specific pair, also get number of basic windows and store the value to the reference */
func getBasicWindowResult(dataMap *(map[int][]Point), granularity int,
  pair *Pair, bwr *BasicWindowResult, bwrdft *BasicWindowDFTResult, isDFT bool, ratio float64) {
  // Pair{leftLocation, rightLocation, i, j}
  leftPointsSlices := (*dataMap)[pair.leftLocation]
  rightPointsSlices := (*dataMap)[pair.rightLocation]
  numberOfBasicwindows := len(leftPointsSlices)/granularity
  var basicWindowIndex int = 0
  // Statistics for basic windows
  var count float64 = 0
  var sumOfX float64 = 0
  var sumOfY float64 = 0
  var sumSquaredX float64 = 0
  var sumSquaredY float64 = 0
  var sumOfXY float64 = 0
  var countOfRemained float64 = 0
  var sumOfXRemained float64 = 0
  var sumOfYRemained float64 = 0
  var sumSquaredXRemained float64 = 0
  var sumSquaredYRemained float64 = 0
  var sumOfXYRemained float64 = 0
  slicesOfMeanX := make([]float64, numberOfBasicwindows)
  slicesOfMeanY := make([]float64, numberOfBasicwindows)
  slicesOfSigmaX := make([]float64, numberOfBasicwindows)
  slicesOfSigmaY := make([]float64, numberOfBasicwindows)
  slicesOfCXY := make([]float64, numberOfBasicwindows)
  slicesOfDXY := make([]float64, numberOfBasicwindows)
  // Slices for DFT
  slicesOfRemainedX := make([]float64, granularity)
  slicesOfRemainedY := make([]float64, granularity)
  slicesOfSumSquaredX := make([]float64, numberOfBasicwindows)
  slicesOfSumSquaredY := make([]float64, numberOfBasicwindows)
  // Compute basic window statistics
  for k := 0; k < len(leftPointsSlices); k += 1 {
    if isDFT {
      slicesOfRemainedX[int(countOfRemained)] = leftPointsSlices[k].temperature
      slicesOfRemainedY[int(countOfRemained)] = rightPointsSlices[k].temperature
    }
    countOfRemained += 1
    sumOfXRemained += leftPointsSlices[k].temperature
    sumOfYRemained += rightPointsSlices[k].temperature
    sumSquaredXRemained += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
    sumSquaredYRemained += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
    sumOfXYRemained += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
    if int(countOfRemained) == granularity {
      var sigmaX float64 = math.Sqrt((sumSquaredXRemained/countOfRemained) - (sumOfXRemained*sumOfXRemained)/(countOfRemained*countOfRemained))
      var sigmaY float64 = math.Sqrt((sumSquaredYRemained/countOfRemained) - (sumOfYRemained*sumOfYRemained)/(countOfRemained*countOfRemained))
      var cXY float64 = (countOfRemained*sumOfXYRemained - sumOfXRemained*sumOfYRemained)/
                        (math.Sqrt(countOfRemained*sumSquaredXRemained - sumOfXRemained*sumOfXRemained)*
                        math.Sqrt(countOfRemained*sumSquaredYRemained - sumOfYRemained*sumOfYRemained))
      if (countOfRemained*sumOfXYRemained - sumOfXRemained*sumOfYRemained) == 0 {
        cXY = 0
      }
      // Update statistics
      count += countOfRemained
      sumOfX += sumOfXRemained
      sumOfY += sumOfYRemained
      sumSquaredX += sumSquaredXRemained
      sumSquaredY += sumSquaredYRemained
      sumOfXY += sumOfXYRemained
      slicesOfMeanX[basicWindowIndex] = sumOfXRemained/countOfRemained
      slicesOfMeanY[basicWindowIndex] = sumOfYRemained/countOfRemained
      slicesOfSigmaX[basicWindowIndex] = sigmaX
      slicesOfSigmaY[basicWindowIndex] = sigmaY
      slicesOfCXY[basicWindowIndex] = cXY
      if isDFT {
        N := int(float64(granularity)*ratio)
        slicesDFTX := make([]complex128, N)
        slicesDFTY := make([]complex128, N)
        getDFTResult(sigmaX, sumOfXRemained/countOfRemained, granularity, N, &slicesOfRemainedX, &slicesDFTX)
        getDFTResult(sigmaY, sumOfYRemained/countOfRemained, granularity, N, &slicesOfRemainedY, &slicesDFTY)
        d := getEuclideanDistance(&slicesDFTX, &slicesDFTY)
        slicesOfDXY[basicWindowIndex] = d
        // For DFT updates
        slicesOfSumSquaredX[basicWindowIndex] = sumSquaredXRemained
        slicesOfSumSquaredY[basicWindowIndex] = sumSquaredYRemained
      }
      // Reset remained values
      countOfRemained = 0
      sumOfXRemained = 0
      sumOfYRemained = 0
      sumSquaredXRemained = 0
      sumSquaredYRemained = 0
      sumOfXYRemained = 0
      // Basic Window Index increment
      basicWindowIndex += 1
    }
  }
  if !isDFT {
    bwr.pair = *pair
    bwr.slicesOfMeanX = &slicesOfMeanX
    bwr.slicesOfMeanY = &slicesOfMeanY
    bwr.slicesOfSigmaX = &slicesOfSigmaX
    bwr.slicesOfSigmaY = &slicesOfSigmaY
    bwr.slicesOfCXY = &slicesOfCXY
  } else {
    bwrdft.pair = *pair
    bwrdft.slicesOfMeanX = &slicesOfMeanX
    bwrdft.slicesOfMeanY = &slicesOfMeanY
    bwrdft.slicesOfSigmaX = &slicesOfSigmaX
    bwrdft.slicesOfSigmaY = &slicesOfSigmaY
    bwrdft.slicesOfDXY = &slicesOfDXY
    bwrdft.slicesOfSumSquaredX = &slicesOfSumSquaredX
    bwrdft.slicesOfSumSquaredY = &slicesOfSumSquaredY
  }
}

/* Sketching part for TSUBASA */
func getBasicWindows(dataMap *(map[int][]Point), granularity int, 
  db *sql.DB, id *int, blockSize int, tableName string, header string, isDFT bool, ratio float64) {
  // Get locations
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  // Nested loops
  var i, j int
  *id = 0 // Set *id to 0
  var accumulate int = 0
  blockInsertionSQLStarter := fmt.Sprintf("INSERT INTO %s %s VALUES ", tableName, header)
  var statementSB strings.Builder
  statementSB.WriteString(blockInsertionSQLStarter)
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      var pair Pair = Pair{leftLocation, rightLocation, i, j}
      var bwr BasicWindowResult
      var bwrdft BasicWindowDFTResult
      if !isDFT {
        getBasicWindowResult(dataMap, granularity, &pair, &bwr, nil, isDFT, ratio)
      } else {
        getBasicWindowResult(dataMap, granularity, &pair, nil, &bwrdft, isDFT, ratio)
      }
      if blockSize <= 0 {
        if !isDFT {
          insertRowBWR(db, &bwr, *id, tableName)
        } else {
          insertRowBWRDFT(db, &bwrdft, *id, tableName)
        }
      } else {
        // Accumulate
        if accumulate > 0 {
          statementSB.WriteString(",")
        }
        if !isDFT {
          appendRowBWR(&statementSB, &bwr, *id)
        } else {
          appendRowBWRDFT(&statementSB, &bwrdft, *id)
        }
        accumulate += 1
        if accumulate == blockSize {
          // Insert rows
          statementSB.WriteString(";")
          insertRowsBWR(db, &statementSB)
          // Reset values
          accumulate = 0
          statementSB.Reset()
          statementSB.WriteString(blockInsertionSQLStarter)
        }
      }
      (*id) += 1 // id increment
    }
  }
  if blockSize > 0 && accumulate > 0 {
    // Insert remained rows
    statementSB.WriteString(";")
    insertRowsBWR(db, &statementSB)
  }
}

/* TSUBASA */
func networkConstructionBW(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
  writeBlockSize int, readBlockSize int, isDFT bool, ratio float64, queryStart int, queryEnd int) {
  // Create a new database
  createNewDB(dbname)
  dbName := dbname
  db := openDB(&dbName) // Open and get the new database
  var tableName, schema, header string
  if (!isDFT) {
    tableName = tablename
    schema = pairsbwrschema
    header = pairsbwrheader
  } else {
    tableName = tablenamedft
    schema = pairsbwrdftschema
    header = pairsbwrdftheader
  }
  createTable(db, tableName, schema) // Create a new table for mapping pairs to statistics
  
  /* Sketch part */
  t0 := time.Now()
  var id int = 0
  var numberOfBasicwindows int = getNumberOfBasicwindows(dataMap, granularity)
  // Store basic window statistics into database
  getBasicWindows(dataMap, granularity, db, &id, writeBlockSize, tableName, header, isDFT, ratio)
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)

  // Check queryStart and queryEnd
  if queryEnd >= 0 {
      if queryEnd - queryStart > numberOfBasicwindows {
      panic("ERROR: queryEnd - queryStart > numberOfBasicwindows")
    }
  }

  /* Query part */
  t1 := time.Now()
  var readTime float64 = 0
  // Read by blocks
  startID := 0
  endID := 0
  for startID < id {
    if startID + readBlockSize > id {
      endID = id
    } else {
      endID = startID + readBlockSize
    }
    readTimeStr := queryRowsDB(db, tableName, startID, endID, matrix, thres, numberOfBasicwindows, isDFT, queryStart, queryEnd)
    readTime += stringToSeconds(readTimeStr)
    startID = endID
  }
  elapsed = time.Since(t1)
  fmt.Println("Query time: ", elapsed)
  deleteTable(db, tableName) // Delete the table
  closeDB(db) // Close the database

  // Delete the database
  deleteDB(dbname)
}

/* Direct calculation network construction */
func networkConstructionNaive(dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  sumOfConnectedPairs := 0
  var i, j int
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      leftPointsSlices := (*dataMap)[leftLocation]
      rightPointsSlices := (*dataMap)[rightLocation]
      var count float64 = 0
      var sumOfX float64 = 0
      var sumOfY float64 = 0
      var sumSquaredX float64 = 0
      var sumSquaredY float64 = 0
      var sumOfXY float64 = 0
      var k int
      for k = 0; k < len(leftPointsSlices); k += 1 {
        count += 1
        sumOfX += leftPointsSlices[k].temperature
        sumOfY += rightPointsSlices[k].temperature
        sumSquaredX += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
        sumSquaredY += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
        sumOfXY += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
      }
      std := ((sumOfXY/count) - (sumOfX*sumOfY)/(count*count))/
      (math.Sqrt((sumSquaredX/count) - ((sumOfX*sumOfX)/(count*count)))*
        math.Sqrt((sumSquaredY/count) - ((sumOfY*sumOfY)/(count*count))))
      if math.Abs(std) >= thres {
        (*matrix)[i][j] = 1
        (*matrix)[j][i] = 1
        sumOfConnectedPairs += 1
      }
    }
  }
  fmt.Println(sumOfConnectedPairs)
}

/* In-memory network construction */
func networkConstructionBWInMemo(dataMap *(map[int][]Point), matrix *([][]int), 
  thres float64, granularity int, isDFT bool, ratio float64, sktechTime *float64, queryTime *float64) {
  var pairWindowsMap map[Pair]BasicWindowResult
  pairWindowsMap = make(map[Pair]BasicWindowResult)
  var pairWindowsMapDFT map[Pair]BasicWindowDFTResult
  pairWindowsMapDFT = make(map[Pair]BasicWindowDFTResult)

  // Get locations
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)

  // Sketch Part
  // Nested loops
  t0 := time.Now()
  var i, j int
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      var pair Pair = Pair{leftLocation, rightLocation, i, j}
      var bwr BasicWindowResult
      var bwrdft BasicWindowDFTResult
      if !isDFT {
        getBasicWindowResult(dataMap, granularity, &pair, &bwr, nil, isDFT, ratio)
        pairWindowsMap[pair] = bwr
      } else {
        getBasicWindowResult(dataMap, granularity, &pair, nil, &bwrdft, isDFT, ratio)
        pairWindowsMapDFT[pair] = bwrdft
      }
    }
  }
  elapsed := time.Since(t0)
  *sktechTime = stringToSeconds(fmt.Sprintf("%v", elapsed))
  fmt.Println("Sketch time: ", elapsed)

  // Query Part
  t1 := time.Now()
  if !isDFT {
    for pair := range pairWindowsMap {
      bwr := pairWindowsMap[pair]
      updateMatrix(matrix, thres, &(bwr.pair), bwr.slicesOfMeanX, bwr.slicesOfMeanY, bwr.slicesOfSigmaX, bwr.slicesOfSigmaY, bwr.slicesOfCXY, nil, false, nil)
    }
  } else {
    for pair := range pairWindowsMapDFT {
      bwrdft := pairWindowsMapDFT[pair]
      updateMatrix(matrix, thres, &(bwrdft.pair), bwrdft.slicesOfMeanX, bwrdft.slicesOfMeanY, bwrdft.slicesOfSigmaX, bwrdft.slicesOfSigmaY, nil, bwrdft.slicesOfDXY, true, nil)
    }
  }
  elapsed = time.Since(t1)
  *queryTime = stringToSeconds(fmt.Sprintf("%v", elapsed))
  fmt.Println("Query time: ", elapsed)
}

func updateSlices(new *([]float64), old *([]float64), coming *([]float64)) {
  for i := 0; i < len(*old) - 1; i += 1 {
    (*new)[i] = (*old)[i+1]
  }
  (*new)[len(*old)-1] = (*coming)[0]
}

func updateBWR(bwrNew *BasicWindowResult, bwrOld *BasicWindowResult, 
  bwrComing *BasicWindowResult) {
  numberOfBasicwindows := len(*(bwrOld.slicesOfMeanX))
  slicesOfMeanX := make([]float64, numberOfBasicwindows)
  slicesOfMeanY := make([]float64, numberOfBasicwindows)
  slicesOfSigmaX := make([]float64, numberOfBasicwindows)
  slicesOfSigmaY := make([]float64, numberOfBasicwindows)
  slicesOfCXY := make([]float64, numberOfBasicwindows)
  //slicesOfDXY := make([]float64, numberOfBasicwindows)
  updateSlices(&slicesOfMeanX, bwrOld.slicesOfMeanX, bwrComing.slicesOfMeanX)
  updateSlices(&slicesOfMeanY, bwrOld.slicesOfMeanY, bwrComing.slicesOfMeanY)
  updateSlices(&slicesOfSigmaX, bwrOld.slicesOfSigmaX, bwrComing.slicesOfSigmaX)
  updateSlices(&slicesOfSigmaY, bwrOld.slicesOfSigmaY, bwrComing.slicesOfSigmaY)
  updateSlices(&slicesOfCXY, bwrOld.slicesOfCXY, bwrComing.slicesOfCXY)

  bwrNew.pair = bwrComing.pair
  bwrNew.slicesOfMeanX = &slicesOfMeanX
  bwrNew.slicesOfMeanY = &slicesOfMeanY
  bwrNew.slicesOfSigmaX = &slicesOfSigmaX
  bwrNew.slicesOfSigmaY = &slicesOfSigmaY
  bwrNew.slicesOfCXY = &slicesOfCXY
}

func updateBWRDFT(bwrNew *BasicWindowDFTResult, bwrOld *BasicWindowDFTResult, 
  bwrComing *BasicWindowDFTResult) {
  numberOfBasicwindows := len(*(bwrOld.slicesOfMeanX))
  slicesOfMeanX := make([]float64, numberOfBasicwindows)
  slicesOfMeanY := make([]float64, numberOfBasicwindows)
  slicesOfSigmaX := make([]float64, numberOfBasicwindows)
  slicesOfSigmaY := make([]float64, numberOfBasicwindows)
  slicesOfDXY := make([]float64, numberOfBasicwindows)
  slicesOfSumSquaredX := make([]float64, numberOfBasicwindows)
  slicesOfSumSquaredY := make([]float64, numberOfBasicwindows)
  updateSlices(&slicesOfMeanX, bwrOld.slicesOfMeanX, bwrComing.slicesOfMeanX)
  updateSlices(&slicesOfMeanY, bwrOld.slicesOfMeanY, bwrComing.slicesOfMeanY)
  updateSlices(&slicesOfSigmaX, bwrOld.slicesOfSigmaX, bwrComing.slicesOfSigmaX)
  updateSlices(&slicesOfSigmaY, bwrOld.slicesOfSigmaY, bwrComing.slicesOfSigmaY)
  updateSlices(&slicesOfDXY, bwrOld.slicesOfDXY, bwrComing.slicesOfDXY)
  updateSlices(&slicesOfSumSquaredX, bwrOld.slicesOfSumSquaredX, bwrComing.slicesOfSumSquaredX)
  updateSlices(&slicesOfSumSquaredY, bwrOld.slicesOfSumSquaredY, bwrComing.slicesOfSumSquaredY)
  bwrNew.pair = bwrComing.pair
  bwrNew.slicesOfMeanX = &slicesOfMeanX
  bwrNew.slicesOfMeanY = &slicesOfMeanY
  bwrNew.slicesOfSigmaX = &slicesOfSigmaX
  bwrNew.slicesOfSigmaY = &slicesOfSigmaY
  bwrNew.slicesOfDXY = &slicesOfDXY
  bwrNew.slicesOfSumSquaredX = &slicesOfSumSquaredX
  bwrNew.slicesOfSumSquaredY = &slicesOfSumSquaredY
}

/* In-memory network construction update */
func networkConstructionBWInMemoUpdate(dataMap *(map[int][]Point), matrix *([][]int), 
  thres float64, granularity int, isDFT bool, ratio float64, dataMapNew *(map[int][]Point)) {
  var pairWindowsMap map[Pair]BasicWindowResult
  pairWindowsMap = make(map[Pair]BasicWindowResult)
  var pairWindowsMapDFT map[Pair]BasicWindowDFTResult
  pairWindowsMapDFT = make(map[Pair]BasicWindowDFTResult)

  // Get locations
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)

  // Sketch Part
  // Nested loops
  t0 := time.Now()
  var i, j int
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      var pair Pair = Pair{leftLocation, rightLocation, i, j}
      var bwr BasicWindowResult
      var bwrdft BasicWindowDFTResult
      if !isDFT {
        getBasicWindowResult(dataMap, granularity, &pair, &bwr, nil, isDFT, ratio)
        pairWindowsMap[pair] = bwr
      } else {
        getBasicWindowResult(dataMap, granularity, &pair, nil, &bwrdft, isDFT, ratio)
        pairWindowsMapDFT[pair] = bwrdft
      }
    }
  }
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)

  accurateMatrix := make([][]float64, len(*dataMap))
  for i := range accurateMatrix {
    accurateMatrix[i] = make([]float64, len(*dataMap))
  }

  // Query Part
  t1 := time.Now()
  if !isDFT {
    for pair := range pairWindowsMap {
      bwr := pairWindowsMap[pair]
      updateMatrix(matrix, thres, &(bwr.pair), bwr.slicesOfMeanX, bwr.slicesOfMeanY, bwr.slicesOfSigmaX, bwr.slicesOfSigmaY, bwr.slicesOfCXY, nil, false, nil)
    }
  } else {
    for pair := range pairWindowsMapDFT {
      bwrdft := pairWindowsMapDFT[pair]
      updateMatrix(matrix, thres, &(bwrdft.pair), bwrdft.slicesOfMeanX, bwrdft.slicesOfMeanY, bwrdft.slicesOfSigmaX, bwrdft.slicesOfSigmaY, nil, bwrdft.slicesOfDXY, true, &accurateMatrix)
    }
  }
  elapsed = time.Since(t1)
  fmt.Println("Query time: ", elapsed)

  t2 := time.Now()
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      var pair Pair = Pair{leftLocation, rightLocation, i, j}
      var bwr, bwrNew BasicWindowResult
      var bwrdft BasicWindowDFTResult
      if !isDFT {
        getBasicWindowResult(dataMapNew, granularity, &pair, &bwr, nil, isDFT, ratio)
        oldBWR := pairWindowsMap[pair]
        updateBWR(&bwrNew, &oldBWR, &bwr)
        updateMatrix(matrix, thres, &(bwrNew.pair), bwrNew.slicesOfMeanX, bwrNew.slicesOfMeanY, bwrNew.slicesOfSigmaX, bwrNew.slicesOfSigmaY, bwrNew.slicesOfCXY, nil, false, nil)
      } else {
        getBasicWindowResult(dataMapNew, granularity, &pair, nil, &bwrdft, isDFT, ratio)
        oldBWRDFT := pairWindowsMapDFT[pair]
        updateMatrixUpdate(matrix, thres, &(oldBWRDFT.pair), oldBWRDFT.slicesOfMeanX, oldBWRDFT.slicesOfMeanY, oldBWRDFT.slicesOfSigmaX, oldBWRDFT.slicesOfSigmaY, nil, oldBWRDFT.slicesOfDXY, oldBWRDFT.slicesOfSumSquaredX, oldBWRDFT.slicesOfSumSquaredY, granularity, &bwrdft, &accurateMatrix)
      }
    }
  }

  elapsed = time.Since(t2)
  fmt.Println("Update time: ", elapsed)
}

/* ---|--------------------|--- */
/* ---| Parallel Computing |--- */
/* ---|____________________|--- */

/* Get the number of CPUs */
func getNumCPU() int {
  return runtime.NumCPU()
}

/* Partition data to NCPU lists */
func partitionData(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair)) {
  // Separate the data map by NCPU
  // The pairs of locations locations are assigned to the list evenly
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  numOfPairs := (locationsNum * (locationsNum - 1)) / 2
  quotient := numOfPairs / NCPU
  remained := numOfPairs % NCPU
  for i := 0; i < NCPU; i += 1 {
    if (i < remained) {
      (*listOfPairs)[i] = make([]Pair, quotient + 1)
    } else {
      (*listOfPairs)[i] = make([]Pair, quotient)
    }
  }
  indexOfRow := 0
  indexOfCol := 1
  for i := 0; i < NCPU; i += 1 {
    for j := 0; j < len((*listOfPairs)[i]); j += 1 {
      (*listOfPairs)[i][j] = Pair{locations[indexOfRow], locations[indexOfCol], indexOfRow, indexOfCol}
      indexOfCol += 1
      if indexOfCol == locationsNum {
        indexOfRow += 1
        indexOfCol = indexOfRow + 1
      }
    }
  }
  fmt.Println(indexOfRow)
  fmt.Println(indexOfCol)
  fmt.Println("Assigned locations: FINISHED")
}

func getBatchesNum(partitionsNum int, listOfPairs *([][]Pair), blockSize int) int {
  var res int = 0
  for i := 0; i < partitionsNum; i += 1 {
    var length int = len((*listOfPairs)[i])
    if length % blockSize == 0 {
      res += length / blockSize
    } else {
      res += length / blockSize + 1
    }
  }
  return res
}

/* writer worker */
func writeDBFromChan(partitionsNum int, dataChan chan DataOfChannel, sem_2 chan int, batchesNum int) {
  dbName := fmt.Sprintf("%s", dbname)
  db := openDB(&dbName) // Open and get the database

  t0 := time.Now()
  for i := 0; i < batchesNum; i += 1 {
    data := <- dataChan
    insertRowsBWRString(db, &(data.statement))
  }
  elapsed := time.Since(t0)
  fmt.Println("Time for writing data: ", elapsed)

  closeDB(db)

  sem_2 <- 1
}

/* DoAll for naive implementation */
func doAllNaive(NCPU int, dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  sem := make(chan int, NCPU)

  // Separate the data map by NCPU
  // The pairs of locations locations are assigned to the list evenly
  listOfPairs := make([][]Pair, NCPU)
  partitionData(NCPU, dataMap, &listOfPairs)

  // doPart
  for i := 0; i < NCPU; i += 1 {
    go doPartNaive(sem, i, &listOfPairs, dataMap, matrix, thres)
  }

  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks are finished.")
}

/* DoPart for naive implementation */
func doPartNaive(sem chan int, taskNum int, listOfPairs *([][]Pair), dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  for i := 0; i < len((*listOfPairs)[taskNum]); i += 1 {
    pair := (*listOfPairs)[taskNum][i]
    leftPointsSlices := (*dataMap)[pair.leftLocation]
    rightPointsSlices := (*dataMap)[pair.rightLocation]
    var count float64 = 0
    var sumOfX float64 = 0
    var sumOfY float64 = 0
    var sumSquaredX float64 = 0
    var sumSquaredY float64 = 0
    var sumOfXY float64 = 0
    var k int
    for k = 0; k < len(leftPointsSlices); k += 1 {
      count += 1
      sumOfX += leftPointsSlices[k].temperature
      sumOfY += rightPointsSlices[k].temperature
      sumSquaredX += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
      sumSquaredY += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
      sumOfXY += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
    }
    std := ((sumOfXY/count) - (sumOfX*sumOfY)/(count*count))/
    (math.Sqrt((sumSquaredX/count) - ((sumOfX*sumOfX)/(count*count)))*
      math.Sqrt((sumSquaredY/count) - ((sumOfY*sumOfY)/(count*count))))
    if math.Abs(std) >= thres {
      (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
      (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
    }
  }
  // Signal that the part is done
  sem <-1
}

/* DoAll for TSUBASA sketch */
func doAllBWSketch(partitionsNum int, dataMap *(map[int][]Point), listOfPairs *([][]Pair),
  granularity int, writeBlockSize int, header string, isDFT bool, ratio float64, durations *([]string)) {

  sem_1 := make(chan int, partitionsNum) // To signal parts are finsihed
  sem_2 := make(chan int, 1)             // To signal writing is finished

  // Compute the number of data batches
  batchesNum := getBatchesNum(partitionsNum, listOfPairs, writeBlockSize)

  dataChan := make(chan DataOfChannel, batchesNum)

  // doPart
  for i := 0; i < partitionsNum; i += 1 {
    go doPartBWSketch(sem_1, dataChan, i, listOfPairs, dataMap, granularity, writeBlockSize, header, isDFT, ratio, durations)
  }

  // writer worker
  go writeDBFromChan(partitionsNum, dataChan, sem_2, batchesNum)

  // Waiting for tasks to be finished
  for i := 0; i < partitionsNum; i += 1 {
    <-sem_1
  }

  // Waiting for writing to be finished
  for i := 0; i < 1; i += 1 {
    <-sem_2
  }

  fmt.Println("All tasks for sketching are finished.")
}

/* DoPart for TSUBASA sketch */
func doPartBWSketch(endChan chan int, dataChan chan DataOfChannel, taskNum int, listOfPairs *([][]Pair), dataMap *(map[int][]Point), 
  granularity int, writeBlockSize int, header string, isDFT bool, ratio float64, durations *([]string)) {
  t0 := time.Now()

  // Open db
  dbName := fmt.Sprintf("%s", dbname)
  db := openDB(&dbName) // Open and get the database

  var accumulate int = 0
  var tableName string
  if !isDFT {
    tableName = fmt.Sprintf("%s_%d", tablename, taskNum)
  } else {
    tableName = fmt.Sprintf("%s_%d", tablenamedft, taskNum)
  }
  blockInsertionSQLStarter := fmt.Sprintf("INSERT INTO %s %s VALUES ", tableName, header)
  var statementSB strings.Builder
  statementSB.WriteString(blockInsertionSQLStarter)

  pairs := (*listOfPairs)[taskNum]
  lengthOfPairs := len(pairs)

  for i := 0; i < lengthOfPairs; i += 1 {
    pair := pairs[i]
    var bwr BasicWindowResult
    var bwrdft BasicWindowDFTResult
    //getBasicWindowResult(dataMap, granularity, &pair, &bwr)
    if !isDFT {
      getBasicWindowResult(dataMap, granularity, &pair, &bwr, nil, false, 0)
    } else {
      getBasicWindowResult(dataMap, granularity, &pair, nil, &bwrdft, true, ratio)
    }

    if writeBlockSize <= 0 {
      if !isDFT {
        insertRowBWR(db, &bwr, i, tableName) // i is id
      } else {
        insertRowBWRDFT(db, &bwrdft, i, tableName) // i is id
      }
    } else {
      // Accumulate
      if accumulate > 0 {
        statementSB.WriteString(",")
      }
      if !isDFT {
          appendRowBWR(&statementSB, &bwr, i) // i is id
        } else {
          appendRowBWRDFT(&statementSB, &bwrdft, i) // i is id
        }
      accumulate += 1
      if accumulate == writeBlockSize {
        // Insert rows
        statementSB.WriteString(";")
        // Not do insertion, but add string to channel
        dataChan <- DataOfChannel{statementSB.String()}
        //insertRowsBWR(db, &statementSB)

        // Reset values
        accumulate = 0
        statementSB.Reset()
        statementSB.WriteString(blockInsertionSQLStarter)
      }
    }
  }
  if writeBlockSize > 0 && accumulate > 0 {
    // Insert remained rows
    statementSB.WriteString(";")
    // Not do insertion, but add string to channel
    dataChan <- DataOfChannel{statementSB.String()}
    //insertRowsBWR(db, &statementSB)
  }

  // close db
  closeDB(db)

  elapsed := time.Since(t0)
  (*durations)[taskNum] = fmt.Sprintf("%v", elapsed)

  fmt.Println("Time: ", elapsed)

  // Signal that the part is done
  endChan <-1
}

/* DoAll for TSUBASA query */
func doAllBWQuery(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair),
  matrix *([][]int), thres float64, readBlockSize int, numberOfBasicwindows int, isDFT bool, 
  queryStart int, queryEnd int, durations *([]string), readsTime *([]float64)) {
  sem := make(chan int, NCPU)
  // doPart
  for i := 0; i < NCPU; i += 1 {
    tableName := fmt.Sprintf("%s_%d", tablename, i)
    if isDFT {
      tableName = fmt.Sprintf("%s_%d", tablenamedft, i)
    }
    go doPartBWQuery(sem, i, listOfPairs, matrix, thres, tableName, readBlockSize, numberOfBasicwindows, isDFT, queryStart, queryEnd, durations, readsTime)
  }
  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks for querying are finished.")
}

/* DoPart for TSUBASA query */
func doPartBWQuery(sem chan int, taskNum int, listOfPairs *([][]Pair),
  matrix *([][]int), thres float64, tableName string, readBlockSize int, numberOfBasicwindows int, isDFT bool, 
  queryStart int, queryEnd int, durations *([]string), readsTime *([]float64)) {
  t0 := time.Now()

  // Open db
  dbName := fmt.Sprintf("%s", dbname)
  db := openDB(&dbName) // Open and get the database

  var totalCnt int = len((*listOfPairs)[taskNum])
  var readTime float64 = 0
  // Read by blocks
  startID := 0
  endID := 0
  for startID < totalCnt {
    if startID + readBlockSize > totalCnt {
      endID = totalCnt
    } else {
      endID = startID + readBlockSize
    }
    readTimeStr := queryRowsDB(db, tableName, startID, endID, matrix, thres, numberOfBasicwindows, isDFT, queryStart, queryEnd)
    //fmt.Println("read: ", readTimeStr)
    readTime += stringToSeconds(readTimeStr)
    startID = endID
  }

  // close db
  closeDB(db)

  elapsed := time.Since(t0)
  (*durations)[taskNum] = fmt.Sprintf("%v", elapsed)
  (*readsTime)[taskNum] = readTime

  // Signal that the part is done
  sem <-1
}

/* Construct network for naive implemetation with parallel computing */
func networkConstructionNaiveParallel(dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  NCPU := getNumCPU()
  fmt.Println("CPU Num: ", NCPU)
  runtime.GOMAXPROCS(NCPU)
  doAllNaive(NCPU, dataMap, matrix, thres)
}

/* Construct network for naive implemetation with parallel computing */
func networkConstructionBWParallel(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
  writeBlockSize int, readBlockSize int, isDFT bool, ratio float64, 
  queryStart int, queryEnd int, sketchDurations *([]string), queryDurations *([]string), queryReadTime *([]float64)) {
  NCPU := getNumCPU()
  fmt.Println("CPU Num: ", NCPU)
  partitionsNum := NCPU - 1
  fmt.Println("Partions Num: ", partitionsNum)
  runtime.GOMAXPROCS(NCPU)

  // Create a new database
  dbName := fmt.Sprintf("%s", dbname)
  createNewDB(dbName)
  db := openDB(&dbName) // Open and get the database

  // Create partitionsNum tables
  for i := 0; i < partitionsNum; i += 1 {
    if !isDFT {
      tableName := fmt.Sprintf("%s_%d", tablename, i)
      createTable(db, tableName, pairsbwrschema) // Create a new table for mapping pairs to basic window statistics
    } else {
      tableName := fmt.Sprintf("%s_%d", tablenamedft, i)
      createTable(db, tableName, pairsbwrdftschema) // Create a new table for mapping pairs to basic window statistics
    }
  }

  // Close db before parallel
  closeDB(db)

  sizeBeforeSketch := getSizeOfDB(dbName)

  var numberOfBasicwindows int = getNumberOfBasicwindows(dataMap, granularity)
  listOfPairs := make([][]Pair, partitionsNum)
  partitionData(partitionsNum, dataMap, &listOfPairs)

  t0 := time.Now()
  header := pairsbwrheader
  if isDFT {
    header = pairsbwrdftheader
  }
  doAllBWSketch(partitionsNum, dataMap, &listOfPairs, granularity, writeBlockSize, header, isDFT, ratio, sketchDurations)
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)

  // Check queryStart and queryEnd
  if queryEnd >= 0 {
      if queryEnd - queryStart > numberOfBasicwindows {
      panic("ERROR: queryEnd - queryStart > numberOfBasicwindows")
    }
  }

  sizeAfterSketch := getSizeOfDB(dbName)
  fmt.Println(fmt.Sprintf("sizeBeforeSketch: %d bytes, sizeAfterSketch: %d bytes, size: %d bytes", sizeBeforeSketch, sizeAfterSketch, sizeAfterSketch - sizeBeforeSketch))

  t1 := time.Now()
  doAllBWQuery(partitionsNum, dataMap, &listOfPairs, matrix, thres, readBlockSize, numberOfBasicwindows, isDFT, queryStart, queryEnd, queryDurations, queryReadTime)
  elapsed = time.Since(t1)
  fmt.Println("Query time: ", elapsed)

  db = openDB(&dbName) 
  // Delete tables
  for i := 0; i < partitionsNum; i += 1 {
    if !isDFT {
      tableName := fmt.Sprintf("%s_%d", tablename, i)
      deleteTable(db, tableName)
    } else {
      tableName := fmt.Sprintf("%s_%d", tablenamedft, i)
      deleteTable(db, tableName)
    }
  }

  closeDB(db) // Close the database
  deleteDB(dbName) // Delete the database
}

func getNetworkInMemo(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, isDFT bool, ratio float64,
  sktechTime *float64, queryTime *float64, totalTime *float64) {
  clearMatrix(matrix)
  t8 := time.Now()
  networkConstructionBWInMemo(dataMap, matrix, thres, granularity, isDFT, ratio, sktechTime, queryTime)
  elapsed := time.Since(t8)
  checkMatrix(matrix)
  fmt.Println("Running time: ", elapsed)
  *totalTime = stringToSeconds(fmt.Sprintf("%v", elapsed))
}

func getDataMap(fileName string, dataMap *(map[int][]Point), before int, numOfLocations int) {
  readErr := ReadLine(fileName, dataMap, before, numOfLocations) // Args:: {3rd: timestamp limit, 4th: number of locations}
  if (readErr != nil) {
    panic(readErr)
  }
  fmt.Println("Length of dataMap: ", len(*dataMap))
}

func main() {
  if len(os.Args) != 15 {
    panic("Invalid number of arguments.")
  }
  // Get parameters from command arguments
  fileName := os.Args[1]
  intVal, _ := strconv.Atoi(os.Args[2])
  var before int = intVal
  intVal, _ = strconv.Atoi(os.Args[3])
  var numOfLocations int = intVal
  floatVal, _ := strconv.ParseFloat(os.Args[4], 64)
  var thres float64 = floatVal
  intVal, _ = strconv.Atoi(os.Args[5])
  var granularity int = intVal
  intVal, _ = strconv.Atoi(os.Args[6])
  var writeBlockSize int = intVal
  intVal, _ = strconv.Atoi(os.Args[7])
  var readBlockSize int = intVal
  floatVal, _ = strconv.ParseFloat(os.Args[8], 64)
  var ratio float64 = floatVal
  intVal, _ = strconv.Atoi(os.Args[9])
  var queryStart int = intVal
  intVal, _ = strconv.Atoi(os.Args[10])
  var queryEnd int = intVal
  var parallel string = os.Args[11]
  var method string = os.Args[12]
  var inMem string = os.Args[13]
  var update string = os.Args[14]
  inputArgs := fmt.Sprintf("fileName: %s, before: %d, numOfLocations: %d, thres: %.2f, granularity: %d, writeBlockSize: %d, readBlockSize: %d, ratio: %.2f, queryStart: %d, queryEnd: %d, parallel: %s, method: %s, inMem: %s, update: %s", 
    fileName, before, numOfLocations, thres, granularity, writeBlockSize, readBlockSize, ratio, queryStart, queryEnd, parallel, method, inMem, update)
  fmt.Println(inputArgs)

  // Read data from *.csv to map, which is stored in memory
  t1 := time.Now()
  dataMap := make(map[int][]Point)
  getDataMap(fileName, &dataMap, before, numOfLocations)

  elapsed := time.Since(t1)
  fmt.Println("Read time: ", elapsed)
  fmt.Println("Read: FINISHED")

  // Matrix initiation
  matrix := make([][]int, len(dataMap))
  for i := range matrix {
    matrix[i] = make([]int, len(dataMap))
  }

  var sketchDurations []string = make([]string, getNumCPU()-1)
  var queryDurations []string = make([]string, getNumCPU()-1)
  var queryReadTime []float64 = make([]float64, getNumCPU()-1)
  var realQueryTime []float64 = make([]float64, getNumCPU()-1)
  var ratioQuery []float64 = make([]float64, getNumCPU()-1)

  // Naive implementation without parallel computing
  if method == "n" && parallel == "f" && update == "f" {
    t2 := time.Now()
    networkConstructionNaive(&dataMap, &matrix, thres)
    elapsed = time.Since(t2)
    fmt.Println("Construction time: ", elapsed)
  }

  // Naive implementation with parallel computing
  if method == "n" && parallel == "t" && update == "f" {
    clearMatrix(&matrix)
    t3 := time.Now()
    networkConstructionNaiveParallel(&dataMap, &matrix, thres)
    elapsed = time.Since(t3)
    checkMatrix(&matrix)
    fmt.Println("Construction time: ", elapsed)
  }

  // TSUBASA without parallel computing, integreted with PostgreSQL
  if parallel == "f" && inMem == "f" {
    clearMatrix(&matrix)
    t4 := time.Now()
    if method == "t" {
      networkConstructionBW(&dataMap, &matrix, thres, granularity, writeBlockSize, readBlockSize, false, ratio, queryStart, queryEnd)
    } else {
      networkConstructionBW(&dataMap, &matrix, thres, granularity, writeBlockSize, readBlockSize, true, ratio, queryStart, queryEnd)
    }
    elapsed = time.Since(t4)
    checkMatrix(&matrix)
    fmt.Println("Running time: ", elapsed)
  }

  // TSUBASA with parallel computing, integreted with PostgreSQL
  if parallel == "t" && inMem == "f" && update == "f" {
    clearMatrix(&matrix)
    t5 := time.Now()
    if method == "t" {
      networkConstructionBWParallel(&dataMap, &matrix, thres, granularity, writeBlockSize, readBlockSize, false, ratio, queryStart, queryEnd, &sketchDurations, &queryDurations, &queryReadTime)
    } else {
      networkConstructionBWParallel(&dataMap, &matrix, thres, granularity, writeBlockSize, readBlockSize, true, ratio, queryStart, queryEnd, &sketchDurations, &queryDurations, &queryReadTime)
    }
    elapsed = time.Since(t5)
    checkMatrix(&matrix)
    fmt.Println("Running time: ", elapsed)
    sketchTime := stringToFloatInSlices(sketchDurations)
    queryTime := stringToFloatInSlices(queryDurations)
    for i := 0; i < len(sketchDurations); i += 1 {
      fmt.Println(sketchDurations[i])
    }
    for i := 0; i < len(queryDurations); i += 1 {
      fmt.Println(queryTime[i] - queryReadTime[i])
    }
    for i := 0; i < len(realQueryTime); i += 1{
      realQueryTime[i] = queryTime[i] - queryReadTime[i]
      ratioQuery[i] = realQueryTime[i] / queryTime[i]
    }
    fmt.Println(fmt.Sprintf("Sketch Time Avg: %f", getAvg(&sketchTime)))
    fmt.Println(fmt.Sprintf("Query (+ Read) Time Max: %f", getMax(&queryTime)))
    fmt.Println(fmt.Sprintf("Query Time Max: %f", getMax(&realQueryTime)))
    fmt.Println(fmt.Sprintf("Read Time sum: %f", getSum(&queryReadTime)))
    fmt.Println(fmt.Sprintf("Query Time ratio Avg: %f", getAvg(&ratioQuery)))
  }

  // TSUBASA on single node, in-memory
  if parallel == "f" && inMem == "t" && update == "f" {
    clearMatrix(&matrix)
    var sktechTime, queryTime float64
    t6 := time.Now()
    if method == "t" {
      networkConstructionBWInMemo(&dataMap, &matrix, thres, granularity, false, ratio, &sktechTime, &queryTime)
    } else {
      networkConstructionBWInMemo(&dataMap, &matrix, thres, granularity, true, ratio, &sktechTime, &queryTime)
    }
    elapsed = time.Since(t6)
    checkMatrix(&matrix)
    fmt.Println("Running time: ", elapsed)
  }

  // TSUBASA update
  if update == "t" {
    dataMapNew := make(map[int][]Point)
    getDataMap(fileName, &dataMapNew, granularity, numOfLocations)
    if method == "t" {
      networkConstructionBWInMemoUpdate(&dataMap, &matrix, thres, granularity, false, ratio, &dataMapNew)
    } else if method == "d" {
      networkConstructionBWInMemoUpdate(&dataMap, &matrix, thres, granularity, true, ratio, &dataMapNew)
    }
  }
}
