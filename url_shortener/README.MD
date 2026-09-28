# Design a URL shortener like bit.ly

## Step 1: Gather functional and non functional requirements

### Step 1.1: Functional requirements

- Create an app which creates a shortened url from a long url
  - optional: support custom url
  - option: support expiration date
- Whenever a user types the shortened URL into the address bar of a browser, they should be redirected to the original URL

### Step 1.2: Non functional requirements

- Low latency -> Response time should be as minimum as possible both during shortened url generation and shortened url to longer original url redirection -> roughly 200ms
- How many users will be using my application? Example: 100M daily active users + shorten 1B long urls
- shortened url should be unique
- CAP theorem: An application of this scale can't be supported via a single monolith. We need to break into microservices i.e a distributed system.
  - Our app demands high availability over consistency. So we need to target eventual consistency, since the target is not that a user immediately checks for their shortened url to transformed to the long url, but rather there would be some time in between. We need to target eventual consistency

## Step 2: Core entities

- shortened url
- long url
- user

## Step 3: API creation

- `POST /v1/url` - body: `{source_url: str, custom_url?, expirationDate?}` - Response: `<shortened_url>`
- `GET /v1/url/:short_url` - Response: `<source_url>`

## Step 4: HLD

![High Level Design](<Screenshot 2026-09-04 at 12.03.33 AM.png>)

### Choice of database

Postgres since relatively simple and relational in nature. `short_url` is primary_key for our `URL` table

## Step 5: Low Level Design

We start with the server where we work on the main logic i.e shortening a URL. You will notice every long_url when reduced to a short_url starts with the same prefix `http://bit.ly/` followed by some `hashed_string` which basically denotes that all we need to do is hash the long_url to this shortened hashed_form using some hashing algorithm

### Approach 1: Simple Encoding/Hashing Logic - WONT' work (read the details for explanation)

md5(long_url) -> {short_url, 6} (take first 6 characters) -> send as response

- problem here is that sha1 or md5 generates millions of urls and startig 6 characters might not be unique. possible solution can be to store the first 6 chars along with a long_url as key-value pair
- but this causes another problem, a second url which generates a different md5 checksum might have the same 6 chars, so a hash collision occurs - to resolve it, we can simply regenerate the md5, scan the db for existence, and then store if no collision
- but we already see the problenm, we don't know how many collisions will happen before the first 6 characters are unique,so clearly this is not a viable solution for a large scale application

### Approach 2: Counter Approach - WONT' work for the mentioned scale(read the details for explanation)

- the logic is that for source_url input, we return a counter integer. 1 for first url, 2 for second url and so on
- Pro's: guaranteed uniqueness per request, faster response time since no need to scan DB to check for existence
- Con's: Not scalable for 100M DAU
  - Solution: Use a multiple instances of your server behind a load balancer (round_robin used to redirect requests) - but then each server would have a counter set to 0
  - Solution: Use a distributed cache like Redis which stores the counter. Since single threaded, doesn't matter which service instance hits the Redis cache, we will get a unique and correct counter. CON's: Single point of failure. We can use a redis cluster, but then we hit the same problem of multiple redis instances starting with counter=0

So this solution won't work for the scale as it has a single point of failure - but this is a good starting point
![Counter Approach](<Screenshot 2026-09-04 at 12.41.40 AM.png>)

### Approach 2.1: Split into encryption and decryption microservices

Our non functional requirement tells us that we have 100M daily active users - however not all 100M users are converting a long url to a short url i.e the encryption operation. Majority (around 80%) are doing the decryption operation i.e taking a short url and getting redirected to the long_url. So the logical way is to split out server into 2 microservices, one for encryption and the other for decryption and then scale each microservice separately based on usage. This optimises resource utilisation and narrows down our scope of implementation

![alt text](<Screenshot 2026-09-04 at 1.05.04 AM.png>)

### Approach 3: Using Zookeeper (distributed system coordinator)

- Zookeeper is an open source service for coordination in distibuted systems. Provides centralized services for config management, naming, synchronization, and group services making it easier to manage large clusters. It essentially acts as a monitoring service for all services in a distributed systems
- Keeps all info of all services in-memory in a hierarchical tree like structure
- peristent(irreplacable) and ephermeral nodes (replacable)
- zookeper checks service availability by heartbeat request

#### Solution

**ZOOKEEPER DETAILS**

- Whenever a service gets registered on zookeeper, we get back a workerID. We can use this workerID to generate a unique `snowflake_id`
- Anatomy of a snowflake_id: 64 bit long -> 1st bit is signed (+ve or -ve) -> next 41 bits are timestamp of the server -> next 10 bits are worker_id -> last 12 bits can be any random number sequence
- So to generate the snowflake_id, we put the first signed bit as positive, then next 41 bits are timestamp of the server, next 10 bits are worker_id generated by zookeeper, and for the last 12 bits, we reintroduce counter per instance of encryption service. This gives us a mechanism to generate a unique `snowflake_id` for each and every service instance registered in zookeeper for per request

**IMPLEMENTATION**

- We keep a zookeeper instance specifically for the encryption service(generates a worker_id per instance and each instance gets stored on an `ephemeral` node)
- Every service instance has a counter initially set to 0, and on every request the snowflake_id gets generated locally in that service instance - and we can assure that the `snowflake_id` generated per request will be unique across services (the last 12 bits random would be generated via that counter).This is very reliable because even if zookeeper fails at some point, we don't care since we're not making direct calls to zookeeper
- Now that we have a snowflake_id, we go back to our first approach of doing a md5 checksum of that snowflake_id. The difference here vs the original approach is that, now we're hashing over a very broader spectrum of data to be hashed, and we're in control of most of what gets encrypted. So the possibility of a hash collision reduces drastically, thus making this much more reliable
- The md5 checksum is generated, the first 6 characters are taken, and then `{short_url: checksum_upto_6_chars}` are stored in database
  ![Hybrid approach with Zookeeper](<Screenshot 2026-09-04 at 8.06.15 AM.png>)

- The short_url -> long_url redirection stays as-is with first check in redis, and if not present then hit db
- For custom_url, we do accept it as part of the encryption request, check db for presence of custom_url (we can put this as a key in redis as well for optimised performance). If present, don't allow that request and simply return a 400.
- Have a cron job which periodically monitors db records to check for expired records and deletes them from DB
- While setting `short_url: long_url` REDIS key mapping for redirection, set TTL of the key to be the same as the `expiresAt` of the corresponding DB record - http 302 redirect response
