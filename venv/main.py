import DB
import time

from DB import sqlDB

obj = sqlDB()

sqlCondition = """CurDeliveryAddr not regexp '( +WAY($| +))|( +PKWY($| +)|( +STE +)|( +ST($| +))|(STREET($| +))|( +RD($| +))|( +ROAD($| +))|( +SUITE($| +))|((^UNIT +)|( +UNITE?($| +)))|( *PLACE($| +))|( +PL($| +))|( +TRL($| +))|( +BLVD($| +))|( +PLAZA($| +))|( +PL($| +))|( +RT($| +))|( +ROUTE($| +))|( +CT($| +))|( +PIKE($| +))|( +PKY($| +))|( +PK +)|( +PARKWAY +)|( +HWY($| +))|( +HIGHWAY($| +))|( +LN($| +))|( +LANE($| +))|( +CIRCLE($| +))|( +CIR($| +))|( +AVE($| +))|( +AVENUE($| +))|( +DR($| +))|( +DRIVE($| +)))'
and CurDeliveryAddr not regexp '([0-9]+ +((([Nn]([EWS]|[ews]) +)|([Ee]([NWS]|[nws]) +)|([Ww]([NES]|[nes]) +)|([Ss]([NEW]|[new])))|([NEWS]|[news])) +)'
and CurDeliveryAddr not regexp '([0-9]+ *)?([Pp][.]? *[Oo][.]? *)[Bb][.]?[Oo][.]?[Xx][.]?'
and CurDeliveryAddr not regexp '(([0-9]+ +)?([bB][oO][xX]) +([0-9]+))'"""

start = time.time()
obj.zillowUpdate('testj', 'main_source_file', 'id', 'CurDeliveryAddr','new12', 'where ' + sqlCondition, '(.*)', 1)
end = time.time()
print("execution time ",end-start)