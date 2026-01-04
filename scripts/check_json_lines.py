import json,sys
p=r'c:\Users\admin\Desktop\procurement_pipeline\data\raw_orders\date=2025-12-02\store_id=ST0002\orders_fixed.json'
with open(p,encoding='utf-8') as f:
    for i,line in enumerate(f,1):
        try:
            json.loads(line)
        except Exception as e:
            print(i, e)
            sys.exit(0)
print('all good')
