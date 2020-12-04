// Package hivething wraps the hiveserver2 thrift interface in a few
// related interfaces for more convenient use.
package gohive

import (
	"bytes"
	"context"

	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	inf "github.com/uxff/gohive/tcliservice"

	//"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/apache/thrift/lib/go/thrift"
)

type Connection struct {
	*thrift.TSocket
	thrift  *inf.TCLIServiceClient
	session *inf.TSessionHandle
	// SaslClientFactory sasl.Client
	// SaslClient        sasl.Client
	options     Options
	Ctx         context.Context
	WriteBuffer *bytes.Buffer
	ReadBuffer  *bytes.Buffer
	opened      bool
}

// IsOpen returns if client is opened
func (t *Connection) IsOpen() bool {
	return t.opened
}
func (t *Connection) Open() error {
	return nil
}

func Connect(host, user, passwd string, options Options, authMethod string) (interface{}, error) {

	if authMethod == HIVENOSASL {
		con, err := ConnectWithUser(host, user, passwd, options)
		if err != nil {
			return nil, err
		}
		return con, nil
	}
	if authMethod == HIVESASL {
		con, err := ConnectWithSasl(host, user, passwd, options)
		if err != nil {
			return nil, err
		}
		return con, nil
	}
	return nil, nil

}

// //this func used as nosasl with sqlstdauth
// func ConnectWithUser(host, user, passwd string, options Options) (*Connection, error) {
// 	con, err := Connect(host, user, passwd, options, HIVENOSASL)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return con, nil
// }

// //this func used as sasl with plain
// func ConnectWithSasl(host, user, passwd string, options Options) (*Connection, error) {
// 	con, err := Connect(host, user, passwd, options, HIVESASL)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return con, nil
// }

func ConnectWithUser(host, user, passwd string, options Options) (*Connection, error) {
	con := &Connection{
		Ctx:         context.Background(),
		ReadBuffer:  new(bytes.Buffer),
		WriteBuffer: new(bytes.Buffer),
	}

	//-----------------------------------

	tsock, err := thrift.NewTSocket(host)
	if err != nil {
		return nil, err
	}
	con.TSocket = tsock
	if err := con.TSocket.Open(); err != nil {
		return nil, err
	}
	if con.TSocket == nil {
		return nil, errors.New("nil thrift transport")
	}
	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	client := inf.NewTCLIServiceClientFactory(con, protocol)

	req := inf.NewTOpenSessionReq()
	//-------------------------------------------------------------
	req.ClientProtocol = inf.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V7

	req.Username = &user
	req.Password = &passwd
	session, err := client.OpenSession(req)
	if err != nil {
		return nil, err
	}
	con.thrift = client
	con.options = options
	con.session = session.SessionHandle
	return con, nil
}

// func connectWithSasl(host, user, passwd string, options Options) (*Connection, error) {
// 	con := &Connection{
// 		Ctx:         context.Background(),
// 		ReadBuffer:  new(bytes.Buffer),
// 		WriteBuffer: new(bytes.Buffer),
// 	}

// 	//-----------------------------------
// 	var saslClient sasl.Client
// 	var issasl bool

// 	saslClient = sasl.NewPlainClient("", user, passwd)
// 	issasl = true

// 	tsock, err := thrift.NewTSocket(host)
// 	if err != nil {
// 		return nil, err
// 	}
// 	con.TSocket = tsock
// 	if err := con.TSocket.Open(); err != nil {
// 		return nil, err
// 	}
// 	if con.TSocket == nil {
// 		return nil, errors.New("nil thrift transport")
// 	}
// 	protocol := thrift.NewTBinaryProtocolFactoryDefault()
// 	client := inf.NewTCLIServiceClientFactory(con, protocol)

// 	req := inf.NewTOpenSessionReq()
// 	//-------------------------------------------------------------

// 	if issasl && saslClient != nil {
// 		mech, ir, err := saslClient.Start()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if _, err := con.sendMessage(START, []byte(mech)); err != nil {
// 			return nil, err
// 		}

// 		if _, err := con.sendMessage(OK, ir); err != nil {
// 			return nil, err
// 		}
// 		for {
// 			status, payload, err := con.recvMessage()
// 			if err != nil {
// 				return nil, err
// 			}
// 			if status != OK && status != COMPLETE {
// 				return nil, errors.New(fmt.Sprintf("Bad status:%d %s", status, string(payload)))
// 			}

// 			if status == COMPLETE {
// 				break
// 			}

// 			response, err := saslClient.Next(payload)
// 			if err != nil {
// 				return nil, err
// 			}
// 			con.sendMessage(OK, response)
// 		}
// 	}
// 	// //-----------------------------------------------------------------

// 	req.ClientProtocol = inf.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V10

// 	req.Username = &user
// 	req.Password = &passwd
// 	fmt.Println("%+v", *client)
// 	fmt.Println("req")
// 	fmt.Println(req)
// 	session, err := client.OpenSession(req)
// 	if err != nil {
// 		return nil, err
// 	}
// 	con.thrift = client
// 	con.options = options
// 	con.session = session.SessionHandle
// 	return con, nil
// }

// // Write writes data into local buffer, which will be processed on Flush() called
// func (t *Connection) Write(data []byte) (int, error) {

// 	return t.WriteBuffer.Write(data)
// } // Read reads data from local buffer, will read a new data frame if needed.
// func (t *Connection) Read(buf []byte) (int, error) {

// 	n, err := t.ReadBuffer.Read(buf)
// 	if n > 0 {
// 		return n, err
// 	}
// 	t.ReadFrame()
// 	return t.ReadBuffer.Read(buf)
// }
// func (t *Connection) Flush() error {
// 	length := make([]byte, 4)
// 	binary.BigEndian.PutUint32(length, uint32(t.WriteBuffer.Len()))

// 	frame := make([]byte, 0)
// 	frame = append(frame, length...)
// 	frame = append(frame, t.WriteBuffer.Bytes()...)
// 	_, err := t.TSocket.Write(frame)
// 	if err == nil {
// 		t.WriteBuffer = new(bytes.Buffer)
// 		return nil
// 	}
// 	return err
// }
// func (t *Connection) ReadFrame() error {
// 	header := make([]byte, 4)
// 	_, err := t.TSocket.Read(header)
// 	if err != nil {
// 		return err
// 	}
// 	length := int(binary.BigEndian.Uint32(header))
// 	data := make([]byte, length)
// 	_, err = t.TSocket.Read(data)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = t.ReadBuffer.Write(data)
// 	return err
// }

func (c *Connection) isOpen() bool {
	return c.session != nil
}

// Closes an open hive session. After using this, the
// Connection is invalid for other use.
func (c *Connection) Close() error {
	if c.isOpen() {
		closeReq := inf.NewTCloseSessionReq()
		closeReq.SessionHandle = c.session
		resp, err := c.thrift.CloseSession(closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session: ", resp, err)
		}
		c.TSocket.Close()
		// c.thrift.Transport.Close(c.Ctx)
		c.session = nil

	}

	return nil
}

func (c *Connection) ExecMode(query string, isAsync bool) (*inf.TOperationHandle, error) {
	executeReq := inf.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.session
	executeReq.Statement = query
	executeReq.RunAsync = isAsync
	executeReq.QueryTimeout = DefaultOptions.QueryTimeout

	resp, err := c.thrift.ExecuteStatement(executeReq)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("Error from server: %s", resp.Status.String())
	}

	return resp.OperationHandle, err
}

func (c *Connection) Exec(query string) (*inf.TOperationHandle, error) {
	return c.ExecMode(query, false)
}

func isSuccessStatus(p *inf.TStatus) bool {
	status := p.GetStatusCode()
	return status == inf.TStatusCode_SUCCESS_STATUS || status == inf.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

func (c *Connection) FetchOne(op *inf.TOperationHandle) (rows *inf.TRowSet, hasMoreRows bool, e error) {
	fetchReq := inf.NewTFetchResultsReq()
	fetchReq.OperationHandle = op
	fetchReq.Orientation = inf.TFetchOrientation_FETCH_NEXT
	fetchReq.MaxRows = DefaultOptions.BatchSize

	resp, err := c.thrift.FetchResults(fetchReq)
	if err != nil {
		log.Printf("FetchResults failed: %v\n", err)
		return nil, false, err
	}

	if !isSuccessStatus(resp.Status) {
		log.Printf("FetchResults failed: %s\n", resp.Status.String())
		return nil, false, errors.New("FetchResult failed, status not ok: " + resp.Status.String())
	}

	rows = resp.GetResults() // return *TRowSet{StartRowOffset int64, Rows []*TRow, Columns []*TColumn, BinaryColumns []byte, ColumnCount *int32}

	//log.Println("the fetch rows=", rows)

	// for json debug
	//jret, jerr := json.Marshal(rows)
	//log.Println("json rows=", string(jret), jerr)

	// GetHasMoreRow()没生效，返回总是false
	return rows, resp.GetHasMoreRows(), nil
}

func (c *Connection) GetMetadata(op *inf.TOperationHandle) (*inf.TTableSchema, error) {
	req := inf.NewTGetResultSetMetadataReq()
	req.OperationHandle = op

	resp, err := c.thrift.GetResultSetMetadata(req)

	if err != nil {
		log.Println("GetMetadata failed:", err)
		return nil, err
	}

	schema := resp.GetSchema()

	// for json debug
	//jret, jerr := json.Marshal(schema)
	//log.Println("schema=", string(jret), jerr)

	return schema, nil
}

/*
	Simple Query
	1. Exec
	2. FetchResult
	3. GetMetadata
	4. Convert to map
*/
func (c *Connection) SimpleQuery(sql string) (rets []map[string]interface{}, err error) {
	operate, err := c.ExecMode(sql, true)
	if err != nil {
		return nil, err
	}

	// wait for ok
	status, err := c.WaitForOk(operate)
	if err != nil {
		log.Println("when waiting occur error:", err, " status=", status.String())
		return nil, err
	}

	schema, err := c.GetMetadata(operate)
	if err != nil {
		return nil, err
	}

	/*
		"columns": [
		       {
		           "i64Val": {
		               "values": [
		                   1,
		                   2
		               ],
		               "nulls": "AA=="
		           }
		       },
		       {
		           "stringVal": {
		               "values": [
		                   "f14581122165221",
		                   "t14581122175212"
		               ],
		               "nulls": "AA=="
		           }
		       },
			...
	*/

	// multiple fetch til all result have got
	var recvLen int
	for {
		var rowLen int

		rows, hasMore, err := c.FetchOne(operate)
		if rows == nil || err != nil {
			log.Println("the FetchResult is nil")
			return nil, err
		}

		var batchRets []map[string]interface{}
		batchRets, err = c.FormatRowsAsMap(rows, schema)

		rowLen = len(batchRets)
		rets = append(rets, batchRets...)

		recvLen += rowLen

		// hasMoreRow 没生效
		if !hasMore {
			log.Println("now more rows, this time rowlen=", rowLen, "StartRowOffset=", rows.StartRowOffset)
			//break
		} else {
			log.Println("has more rows, this time rowlen=", rowLen, "StartRowOffset=", rows.StartRowOffset)
		}
		// 需要从返回的数量里判断任务有没有进行完
		if rowLen <= 0 {
			log.Println("no more rows find, rowlen=", rowLen, "all got rowlen=", recvLen)
			break
		}
	}

	//--------------------------------------------------------------
	//仅用于排序
	type sortmap struct {
		fieldname string
		sortnums  int
	}

	if len(rets) != 0 {
		var orderrets = make([]map[string]interface{}, 0)
		for _, v := range rets {
			var ordermap = make(map[string]interface{}, 0)
			for _, f := range schema.GetColumns() {
				ordermap[f.GetColumnName()] = v[f.GetColumnName()]
			}
			orderrets = append(orderrets, ordermap)
		}
		return orderrets, nil
	}
	return rets, nil
}

// Issue a thrift call to check for the job's current status.
func (c *Connection) CheckStatus(operation *inf.TOperationHandle) (*Status, error) {

	req := inf.NewTGetOperationStatusReq()
	req.OperationHandle = operation

	//log.Println("will request GetOperationStatus")

	resp, err := c.thrift.GetOperationStatus(req)
	if err != nil {
		return nil, fmt.Errorf("Error getting status: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("GetStatus call failed: %s", resp.Status.String())
	}

	if resp.OperationState == nil {
		return nil, errors.New("No error from GetStatus, but nil status!")
	}

	//log.Println("OperationStatus", resp.GetOperationState(), "ProgressUpdate=", resp.GetProgressUpdateResponse())

	return &Status{resp.OperationState, nil, time.Now()}, nil
}

// Wait until the job is complete, one way or another, returning Status and error.
func (c *Connection) WaitForOk(operation *inf.TOperationHandle) (*Status, error) {
	for {
		status, err := c.CheckStatus(operation)

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(time.Duration(DefaultOptions.PollIntervalSeconds) * time.Second)
	}
	return nil, errors.New("Cannot run here when wait for operation ok")
}

// Returns a string representation of operation status.
func (s Status) String() string {
	if s.state == nil {
		return "unknown"
	}
	return s.state.String()
}

// Returns true if the job has completed or failed.
func (s Status) IsComplete() bool {
	if s.state == nil {
		return false
	}

	switch *s.state {
	case inf.TOperationState_FINISHED_STATE,
		inf.TOperationState_CANCELED_STATE,
		inf.TOperationState_CLOSED_STATE,
		inf.TOperationState_ERROR_STATE:
		return true
	}

	return false
}

// Returns true if the job compelted successfully.

func (s Status) IsSuccess() bool {
	if s.state == nil {
		return false
	}

	return *s.state == inf.TOperationState_FINISHED_STATE
}

func DeserializeOp(handle []byte) (*inf.TOperationHandle, error) {
	ser := thrift.NewTDeserializer()
	var val inf.TOperationHandle

	if err := ser.Read(&val, handle); err != nil {
		return nil, err
	}

	return &val, nil
}

// func SerializeOp(operation *inf.TOperationHandle) ([]byte, error) {
// 	ser := thrift.NewTSerializer()
// 	return ser.Write(operation)
// }

/*
	将返回数据转换成map
*/
func (c *Connection) FormatRowsAsMap(rows *inf.TRowSet, schema *inf.TTableSchema) (rets []map[string]interface{}, err error) {
	var colValues = make(map[string]interface{}, 0)
	var rowLen int

	for cpos, tcol := range rows.Columns {
		// 此循环内遍历列名 取出所有列下的结果

		colName := schema.Columns[cpos].ColumnName

		switch true {
		case tcol.IsSetBinaryVal():
			colValues[colName] = tcol.GetBinaryVal().GetValues()
			rowLen = len(tcol.GetBinaryVal().GetValues())
		case tcol.IsSetBoolVal():
			colValues[colName] = tcol.GetBoolVal().GetValues()
			rowLen = len(tcol.GetBoolVal().GetValues())
		case tcol.IsSetByteVal():
			colValues[colName] = tcol.GetByteVal().GetValues()
			rowLen = len(tcol.GetByteVal().GetValues())
		case tcol.IsSetDoubleVal():
			colValues[colName] = tcol.GetDoubleVal().GetValues()
			rowLen = len(tcol.GetDoubleVal().GetValues())
		case tcol.IsSetI16Val():
			colValues[colName] = tcol.GetI16Val().GetValues()
			rowLen = len(tcol.GetI16Val().GetValues())
		case tcol.IsSetI32Val():
			colValues[colName] = tcol.GetI32Val().GetValues()
			rowLen = len(tcol.GetI32Val().GetValues())
		case tcol.IsSetI64Val():
			colValues[colName] = tcol.GetI64Val().GetValues()
			rowLen = len(tcol.GetI64Val().GetValues())
		case tcol.IsSetStringVal():
			colValues[colName] = tcol.GetStringVal().GetValues()
			rowLen = len(tcol.GetStringVal().GetValues())
		}
	}

	// 将列结构转换成行结构
	for i := 0; i < rowLen; i++ {
		formatedRow := make(map[string]interface{}, 0)
		for colName, colValueList := range colValues {
			// column => [v1, v2, v3, ...]
			formatedRow[colName] = reflect.ValueOf(colValueList).Index(i).Interface()
		}

		rets = append(rets, formatedRow)
	}

	return rets, nil
}

/*将hive返回的数据转换成内容行 不使用reflect 不占用两份内存*/
func (c *Connection) FormatRows(rows *inf.TRowSet, schema *inf.TTableSchema) (rets [][]string, err error) {
	//var colValues = make(map[string]interface{}, 0)
	var rowLen int
	var colLen = len(rows.Columns)
	//var retsMap []map[string]interface{}

	colNames := make([]string, 0)

	// 以下循环处理后 colValues=[col1=>[line1v1,line2v1,...], col2=>[line1v2,line2v2]]
	// 不知道rowlen,需要循环后才能知道
	for cpos, tcol := range rows.Columns {
		// 此循环内遍历列名 取出所有列下的结果

		colName := schema.Columns[cpos].ColumnName
		colNames = append(colNames, colName)

		switch true {
		case tcol.IsSetStringVal():
			//rowLen = c.convertColsToRows(cpos, colLen, tcol.GetStringVal().GetValues(), &rets)
			valuesOfCol := tcol.GetStringVal().GetValues()
			rowLen = len(valuesOfCol)
			if len(rets) == 0 {
				//log.Println("will remalloc rets", colName)
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					//log.Println("will remalloc rets[", i, "]->", "colName=", colName, "colType=",reflect.TypeOf(oneCell))
					rets[i] = make([]string, colLen)
				}
				//log.Println("set rets[", i, "]->", cpos, colName, "=", oneCell, reflect.TypeOf(oneCell))
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		case tcol.IsSetBinaryVal():
			valuesOfCol := tcol.GetBinaryVal().GetValues()
			rowLen = len(valuesOfCol)
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		case tcol.IsSetBoolVal():
			valuesOfCol := tcol.GetBoolVal().GetValues()
			rowLen = len(valuesOfCol) //len(tcol.GetBoolVal().GetValues())
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		case tcol.IsSetByteVal():
			valuesOfCol := tcol.GetByteVal().GetValues()
			rowLen = len(valuesOfCol) //len(tcol.GetByteVal().GetValues())
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		case tcol.IsSetDoubleVal():
			valuesOfCol := tcol.GetDoubleVal().GetValues()
			rowLen = len(valuesOfCol) //len(tcol.GetDoubleVal().GetValues())
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%g", oneCell)
			}
		case tcol.IsSetI16Val():
			valuesOfCol := tcol.GetI16Val().GetValues()
			rowLen = len(valuesOfCol) //len(tcol.GetI16Val().GetValues())
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		case tcol.IsSetI32Val():
			valuesOfCol := tcol.GetI32Val().GetValues()
			rowLen = len(valuesOfCol) //len(tcol.GetI32Val().GetValues())
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		case tcol.IsSetI64Val():
			valuesOfCol := tcol.GetI64Val().GetValues()
			rowLen = len(valuesOfCol) //len(tcol.GetI64Val().GetValues())
			if len(rets) == 0 {
				rets = make([][]string, rowLen)
			}
			for i, oneCell := range valuesOfCol {
				if rets[i] == nil {
					rets[i] = make([]string, colLen)
				}
				rets[i][cpos] = fmt.Sprintf("%v", oneCell)
			}
		default:
			err = fmt.Errorf("the value is unsupported: %v", tcol)
			log.Println("when format rows:", err)
		}
	}

	log.Println("this fetch will format len=", rowLen, "rets=", rets)

	return rets, nil
	/*
	       rets = make([][]string, rowLen)

	   	// 将列结构转换成行结构
	   	for i := 0; i < rowLen; i++ {
	   		// 遍历列
	           rets[i] = make([]string, len(retsMap[i]))

	           for colNo, colName := range colNames {
	               // 此处oneCellVal为乱序，从retsMap[i]中随机key取出
	               rets[i][colNo] = fmt.Sprintf("%v", retsMap[i][colName])
	           }

	   	}
	*/
	return rets, nil
}

/*返回格式化后的表头*/
func (c *Connection) FormatHeads(schema *inf.TTableSchema) (outHead []string, err error) {
	if schema == nil {
		err = fmt.Errorf("schema is nil shen FormatHeads")
		return
	}
	for _, col := range schema.Columns {
		outHead = append(outHead, col.GetColumnName())
	}
	return
}

func (c *Connection) GetOptions() *Options {
	return &c.options
}
