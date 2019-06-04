import DB
import time

from DB import sqlDB

obj = sqlDB()

# #absolute name select
# sqlCondition = """CurDeliveryAddr not regexp '( +WAY($| +))|( +PKWY($| +)|( +STE +)|( +ST($| +))|(STREET($| +))|( +RD($| +))|( +ROAD($| +))|( +SUITE($| +))|((^UNIT +)|( +UNITE?($| +)))|( *PLACE($| +))|( +PL($| +))|( +TRL($| +))|( +BLVD($| +))|( +PLAZA($| +))|( +PL($| +))|( +RT($| +))|( +ROUTE($| +))|( +CT($| +))|( +PIKE($| +))|( +PKY($| +))|( +PK +)|( +PARKWAY +)|( +HWY($| +))|( +HIGHWAY($| +))|( +LN($| +))|( +LANE($| +))|( +CIRCLE($| +))|( +CIR($| +))|( +AVE($| +))|( +AVENUE($| +))|( +DR($| +))|( +DRIVE($| +)))'
# and CurDeliveryAddr not regexp '([0-9]+ +((([Nn]([EWS]|[ews]) +)|([Ee]([NWS]|[nws]) +)|([Ww]([NES]|[nes]) +)|([Ss]([NEW]|[new])))|([NEWS]|[news])) +)'
# and CurDeliveryAddr not regexp '([0-9]+ *)?([Pp][.]? *[Oo][.]? *)[Bb][.]?[Oo][.]?[Xx][.]?'
# and CurDeliveryAddr not regexp '(([0-9]+ +)?([bB][oO][xX]) +([0-9]+))'"""
#
# start = time.time()
# obj.zillowUpdate('testj', 'main_source_file', 'id', 'CurDeliveryAddr','new12', 'where ' + sqlCondition, '(.*)', 1)
# end = time.time()
# print("execution time ",end-start)
#
# sqlCondition = """CurDeliveryAddr regexp '( +WAY($| +))|( +PKWY($| +)|( +STE +)|( +ST($| +))|(STREET($| +))|( +RD($| +))|( +ROAD($| +))|( +SUITE($| +))|((^UNIT +)|( +UNITE?($| +)))|( *PLACE($| +))|( +PL($| +))|( +TRL($| +))|( +BLVD($| +))|( +PLAZA($| +))|( +PL($| +))|( +RT($| +))|( +ROUTE($| +))|( +CT($| +))|( +PIKE($| +))|( +PKY($| +))|( +PK +)|( +PARKWAY +)|( +HWY($| +))|( +HIGHWAY($| +))|( +LN($| +))|( +LANE($| +))|( +CIRCLE($| +))|( +CIR($| +))|( +AVE($| +))|( +AVENUE($| +))|( +DR($| +))|( +DRIVE($| +)))'
# or CurDeliveryAddr regexp '([0-9]+ +((([Nn]([EWS]|[ews]) +)|([Ee]([NWS]|[nws]) +)|([Ww]([NES]|[nes]) +)|([Ss]([NEW]|[new])))|([NEWS]|[news])) +)'
# or CurDeliveryAddr regexp '([0-9]+ *)?([Pp][.]? *[Oo][.]? *)[Bb][.]?[Oo][.]?[Xx][.]?'
# or CurDeliveryAddr regexp '(([0-9]+ +)?([bB][oO][xX]) +([0-9]+))'"""
#
# start = time.time()
# obj.zillowUpdate('testj', 'main_source_file', 'id', 'CurDeliveryAddr','address_extract', 'where ' + sqlCondition, '(.*)', 1)
# end = time.time()
# print("execution time ",end-start)

################################################
sqlCondition = """ADDRESS1 like '% box %'
and address_extract is null"""
#num po box
regexp = '((([0-9]+ *)?([Pp] *[.,]? *[Oo0] *[,.]? *)[Bb] *[,.]?[Oo] *[.,]?[Xx] *[,.]?).*)'

start = time.time()
obj.zillowUpdate('testj', 'pe_owner', 'id', 'ADDRESS1','address_extract', 'where ' + sqlCondition, regexp, 1)
end = time.time()

#num po .* road/rd No need of it
print("Execution time: ", end-start)

######################################
#num po box
#regexp = '((([0-9]+ *)?(([pP][oO][sS][tT] *[oO][fF][fF][iI][cC][eE])|([Pp] *[,.]? *[Oo0] *[,.]? *)).*(([Rr][dD])|([rR][oO0][aA][dD]))))( *$)'
regexp = '((([0-9]+ *)?(([pP][oO][sS][tT] *[oO][fF][fF][iI][cC][eE])|([Pp][.]? *[Oo][.]? *))[Bb][.]?[Oo0][.]?[Xx][.]?).*)'

start = time.time()
obj.zillowUpdate('testj', 'pe_owner', 'id', 'ADDRESS1','address_extract', 'where ' + sqlCondition, regexp, 1)
end = time.time()
print("Execution time: ", end-start)
#num box .* road/rd
print("Execution time: ", end-start)
#####################################
#regexp = '((([0-9]+ *)?([bB] *[.,]? *[Oo0] *[.,]? *[xX]).*(([Rr][dD])|([rR][oO0][aA][dD]))))( *$)'
regexp = '((([rR][oO]?[uU]?[tT][eE]?) *([0-9]+ *)?([bB] *[.,]? *[Oo0] *[.,]? *[xX]).*(([Rr][dD])|([rR][oO0][aA][dD]))))( *$)'
start = time.time()
obj.zillowUpdate('testj', 'pe_owner', 'id', 'ADDRESS1','address_extract', 'where ' + sqlCondition, regexp, 1)
end = time.time()
print("Execution time: ", end-start)
#####################################

# Name extract

regexp = '(.*) +((([0-9]+ *)((([rR][oO]?[uU]?[tT][eE]?) *([0-9]+ *)?([bB] *[.,]? *[Oo0] *[.,]? *[xX]).*(([Rr][dD])|([rR][oO0][aA][dD]))))( *$)))'
sqlCondition = """ADDRESS1 like '% box %'
and name_extract is null"""
start = time.time()
obj.zillowUpdate('testj', 'pe_owner', 'id', 'ADDRESS1','name_extract', 'where ' + sqlCondition, regexp, 1)
end = time.time()
print("Execution time: ", end-start)