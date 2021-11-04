import redis

r = redis.Redis(host='localhost', port=6379, db=0)

print(r.keys())


# #print(r.hgetall('opt.supervisorcore.72702108.tasks.status').keys())
# print(r.hgetall('opt.supervisorcore.1591912127.tasks.status').keys())
#
# all_keys = list(r.hgetall('opt.supervisorcore.1591912127.tasks.status').keys())
# for k in all_keys:
#     #print(k)
#     r.hdel('opt.supervisorcore.1591912127.tasks.status', k)
#
# print(r.hgetall('opt.supervisorcore.1591912127.tasks.status').keys())
