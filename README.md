# core-locks

It provides a LockManager based on the redis database, which implements a distributed locks 
that automatically renews the lease in a timely manner. The lease is attached to a TTL (time to live interval). 
When the server goes down or when the context is canceled, the locks created on top of the lease, 
will be gone with the lease.
