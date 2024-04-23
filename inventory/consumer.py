from main import redis, Product
import time

key = 'order_completed'
group = 'inventory-group'

try:
    redis.xgroup_create(key, group)
except Exception as e:
    print(f"Failed to create group: {str(e)}")

while True:
    try:
        results = redis.xreadgroup(group, key, {key: '>'}, None)

        if results:
            for result in results:
                obj = result[1][0][1]
                try:
                    product = Product.get(obj['product_id'])
                    product.quantity -= int(obj['quantity'])
                    product.save()
                except Exception as e:
                    print(f"Error processing order: {str(e)}")
                    redis.xadd('refund_order', obj, '*')
    except Exception as e:
        print(f"Error reading stream: {str(e)}")

    time.sleep(1)
