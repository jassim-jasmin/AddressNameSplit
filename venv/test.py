from DB import sqlDB

def con():
    sqlCondition = "regexp '[cC][\/][oO0]'"
    obj = sqlDB()
    obj.defaultZillow()
    obj.connectSchema('testj')
    i = obj.getId('testExtraction', 'id', 'test1', 'test2', sqlCondition)
    j = obj.getConcatId('testExtraction', 'id', 'test1', 'test2', i)
    obj.createColumn('testExtraction', 'test1' + '_co_name')
    obj.insertData('testExtraction', 'id', 'test1' + '_co_name', j)

def extraction():
    extrac = Extraction()

    schemaName = 'testj'
    tableName = 'testExtraction'
    idColumnName = 'id'
    fieldName = 'test2'

    # schemaName = 'ga_statewide_renewal' #'testj'
    # tableName = 'pe_owner'
    # idColumnName = 'id'
    # fieldName = 'test_ADDRESS1'

    # schemaName = 'fl_flagler_rawdata' #'testj'
    # schemaName = 'testj'
    # tableName = 'testExtraction'
    # idColumnName = 'id'
    # fieldName = 'ADDRESS_2'

    print(schemaName, tableName)
    start = time.time()
    extrac.orderedExtractZillow(schemaName, tableName, idColumnName, fieldName)
    # fieldName = 'ADDRESS_1'
    # extrac.orderedExtractZillow(schemaName, tableName, idColumnName, fieldName)
    # fieldName = 'ADDRESS_3'
    # extrac.orderedExtractZillow(schemaName, tableName, idColumnName, fieldName)
    end = time.time()
    print("Complete execution time: ", end - start)