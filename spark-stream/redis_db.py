import redis
import sys

#redisdb to store matches in an atomic way using multi transaction mechanism
class RedisDB:

	def __init__(self):
		self.redishost = 'ip-10-0-0-10'
		self.redisport = xxxx
		self.redispasswd = xxxx
    		self.rs_driverdb = redis.StrictRedis(host=self.redishost,  port=self.redisport, db=1, password=self.redispasswd)
    		self.rs_senderdb = redis.StrictRedis(host=self.redishost,  port=self.redisport, db=2, password=self.redispasswd)

	def flush_db(self):
		print("flushing redisdb")
		self.rs_driverdb.flushdb()
		self.rs_driverdb.flushdb()

	# secure driver or sender resource in redis
	def secure_driver_util(self,driverid):

		# redis watch transaction ensures that 
		# the record we want to write if claimed by some else
		# we get notified so to avoid overbooking scenario
		ret_val = False
		
		pipe = self.rs_driverdb.pipeline()
		pipe.watch(driverid)
		if (str(self.rs_driverdb.get(driverid)) == 'None'):
			pipe.multi()
			pipe.set(driverid, 1)
			try: 
				pipe.execute()
				ret_val = True
			except:
				ret_val = False

		return ret_val

	# helper function claim driver upon match
	def secure_best_driver(self,driverList):
		id_idx = 0
		for driver in driverList:
			# print "Driver: "+ str(driver)
			if(secure_driver_util(driver['id'],'driver',r_db) == True):
					return (driver, id_idx)
			id_idx = id_idx + 1

		return ('None', -1)

	# commit sender record
	def commit_sender(self,id):
   		self.rs_senderdb.set(id, 1)


def main():
    # print command line arguments

    redis_db = RedisDB()
    if(len(sys.argv) == 2):
	if(sys.argv[1] == "flush"):
		redis_db.flush_db()
	else:
		print("WRONG OPTIONS. PROVIDE flush option")
    else:
	print("PROVIDE flush as options")
	

if __name__ == "__main__":
    main()
