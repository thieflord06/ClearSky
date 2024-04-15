# API Documentation

## Response Errors:
- **400:** Bad Request
- **404:** Not Found
- **500:** Internal Server Error
- **503:** Service Unavailable

## Pagination:
 `<page:int>`
- **Value:** Integer 
- This is a parameter that is used to paginate the results. It is used to specify the page number of the results to be returned. The default value is 1.

## 1.

- **Endpoint:** `/api/v1/anon/blocklist/<handle/did>/<page:int>`
- **Method:** `GET`
- **Description:** Get list of users that someone is blocking
- **Parameters:** handle or did
- **Response:**
    ```json
        {
        "data":
            {
                "blocklist":
                    [
                        {"blocked_date":"2024-03-26T16:25:45.414000+00:00","handle":"lowqualityfacts.bsky.social","status":true},
                        {"blocked_date":"2024-03-17T21:17:44.607000+00:00","handle":"acreepyphilosopher.bsky.social","status":true},
                        {"blocked_date":"2024-03-14T11:17:43.670000+00:00","handle":"danys.bsky.social","status":true},
                        ...
                    ],
                    "count":"388",
                    "pages":4
            },
                "identity":"rudyfraser.com",
                "status":true
        }

## 2.

- **Endpoint:** `/api/v1/anon/single-blocklist/<handle/did>/<page:int>`
- **Method:** `GET`
- **Description:** Get list of users that someone is blocked by
- **Parameters:** handle or did
- **Response:**
    ```json
        {
            "data":
                {
                    "blocklist":
                        [
                            {"blocked_date":"2024-04-08T01:20:28.071211+00:00","handle":"philcollins.bsky.social","status":false},
                            {"blocked_date":"2024-04-07T02:45:47.695801+00:00","handle":"kinew.bsky.social","status":false},
                            {"blocked_date":"2024-03-23T12:49:13.887000+00:00","handle":"23-march-2024.bsky.social","status":true},
                            ...
                        ],
                        "count":"292",
                        "pages":3
                },
                    "identity":"rudyfraser.com",
                    "status":true
        }

## 3.

- **Endpoint:** `/api/v1/anon/get-list/<handle/did>`
- **Method:** `GET`
- **Description:** Get list of moderation lists a user is on
- **Parameters:** handle or did
- **Response:**
    ```json
          {
              "data":
                  {
                      "identifier":"rudyfraser.com",
                          "lists":
                              [
                                  {"created_date":"2023-11-10T14:48:41.854000+00:00","date_added":"2023-12-07T03:29:19.984000+00:00","description":"To see only porn, block or mute everyone on this list. Ezpz.","handle":"whydoilikethis.bsky.social","list user count":17065,"name":"Not Porn","status":true,"url":"https://bsky.app/profile/did:plc:g2xshwj4o33b5wzxs3xspfxk/lists/3kdtppevxdt2h"},
                                  {"created_date":"2023-07-02T23:46:37.719000+00:00","date_added":"2024-02-24T18:02:24.691000+00:00","description":"Bas3d crypto skeeters","handle":"alkohlmist.bsky.social","list user count":104,"name":"MetaDAOists","status":true,"url":"https://bsky.app/profile/did:plc:dwzmn37bquliunjanpeostli/lists/3jzlaonnxtl2m"},
                                  {"created_date":"2024-02-29T05:41:53.330000+00:00","date_added":"2024-03-01T17:09:31.144000+00:00","description":"","handle":"edavis.dev","list user count":41,"name":"Tech","status":true,"url":"https://bsky.app/profile/did:plc:4nsduwlpivpuur4mqkbfvm6a/lists/3kmjuvrij622k"},
                                  {"created_date":"2023-12-14T13:45:14.015000+00:00","date_added":"2023-12-22T00:45:29.137000+00:00","description":"Your presence here makes a big difference and improves my feeds, thank you!","handle":"scanty.bsky.social","list user count":1449,"name":"Feed Guard \ud83d\udee1\ufe0f","status":true,"url":"https://bsky.app/profile/did:plc:2wt2zjrqtveulc5a2sl2ppgt/lists/3kq3dtinvc32i"},
                                  ...
                              ]
                  },
              "identifier":"rudyfraser.com"
          }

## 4.

- **Endpoint:** `/api/v1/anon/get-did/<handle>`
- **Method:** `GET`
- **Description:** Get the DID of a given handle
- **Parameters:** handle
- **Response:**
    ```json
        {
            "data":
                {
                    "avatar_url":"https://av-cdn.bsky.app/img/avatar/plain/bafkreicbh2mxpza6xhdwfwdvro33jlioue3g4elfp75u3je64dbvjk44la",
                    "did_identifier":"did:plc:w4xbfzo7kqfes5zb7r6qv3rw",
                    "identifier":"rudyfraser.com",
                    "user_url":"https://bsky.app/profile/did:plc:w4xbfzo7kqfes5zb7r6qv3rw"
                }
        }

## 5.

- **Endpoint:** `/api/v1/anon/get-handle/<did>`
- **Method:** `GET`
- **Description:** Get the handle of a given DID
- **Parameters:** DID
- **Response:**
    ```json
        {
            "data":
                {
                    "avatar_url":"https://av-cdn.bsky.app/img/avatar/plain/bafkreicbh2mxpza6xhdwfwdvro33jlioue3g4elfp75u3je64dbvjk44la",
                    "handle_identifier":"rudyfraser.com",
                    "identifier":"did:plc:w4xbfzo7kqfes5zb7r6qv3rw",
                    "user_url":"https://bsky.app/profile/did:plc:w4xbfzo7kqfes5zb7r6qv3rw"
                }
        }

## 6.

- **Endpoint:** `/api/v1/anon/get-handle-history/<handle/did>`
- **Method:** `GET`
- **Description:** Get the account history of a user
- **Parameters:** handle or did
- **Response:**
    ```json
        {
            "data":
                {
                    "handle_history":
                    [
                        ["rudyfraser.com","2023-11-13T02:37:31.907Z","https://shiitake.us-east.host.bsky.network"],
                        ["rudyfraser.com","2023-05-25T12:22:54.104Z","https://bsky.social"],
                        ["salesforce.herokuapp.com","2023-05-04T00:32:48.659Z","https://bsky.social"],
                        ["rudyfraser.herokuapp.com","2023-05-04T00:29:38.207Z","https://bsky.social"],
                        ["rudyfraser.com","2023-05-02T16:05:34.405Z","https://bsky.social"],
                        ["shacqeal.bsky.social","2023-05-01T03:43:42.434Z","https://bsky.social"]
                        ...
                    ],
                    "identifier":"rudyfraser.com"
                }
        }

## 7.

- **Endpoint:** `/api/v1/anon/at-uri/<uri>`
- **Method:** `GET`
- **Description:** Get the URL of a given URI
- **Parameters:** URI
- **Response:**
    ```json
        {
            "data":
            {
                "url": "https://bsky.app/profile/did:plc:bfjoqzne3tm5yxvpybdfahzo/lists/3jzmevbfqkj2u"
            }
        }


## 8.

- **Endpoint:** `/api/v1/anon/blocklist-search-blocking/<handle1>/<handle2>`
- **Method:** `GET`
- **Description:** Get if handle 1 is blocked by handle 2
- **Parameters:** handle 1 & handle 2
- **Response:**
    ```json

## 9.

- **Endpoint:** `/api/v1/anon/blocklist-search-blocked/<handle1>/<handle2>`
- **Method:** `GET`
- **Description:** Get if handle 1 blocked by handle 2
- **Parameters:** handle 1 & handle 2
- **Response:**
    ```json

## 10.

- **Endpoint:** `/api/v1/anon/get-moderation-list/<name:string>`
- **Method:** `GET`
- **Description:** Get a list of lists and other details based on word search of either the list name or description
- **Parameters:** name
- **Response:**
    ```json

## 11.

- **Endpoint:** `/api/v1/base/autocomplete/<handle:string>`
- **Method:** `GET`
- **Description:** Get a list of handles based on a partial word search
- **Parameters:** handle (partial/complete)
- **Response:**
    ```json

## 12.

- **Endpoint:** `/api/v1/anon/total-users`
- **Method:** `GET`
- **Description:** Get user count information: total users, active users, deleted users
- **Parameters:** none
- **Response:**
    ```json

## 13.

- **Endpoint:** `/api/v1/anon/lists/fun-facts`
- **Method:** `GET`
- **Description:** Get top 20 blockers and blocked
- **Parameters:** None
- **Response:**
    ```json

## 14.

- **Endpoint:** `/api/v1/anon/lists/funer-facts`
- **Method:** `GET`
- **Description:** Get top 20 blockers and blocked in the last 24 hours
- **Parameters:** None
- **Response:**
    ```json

## 15.

- **Endpoint:** `/api/v1/anon/lists/block-stats`
- **Method:** `GET`
- **Description:** Get a list of block stats based on userbase
- **Parameters:** None
- **Response:**
    ```json

## 16.

- **Endpoint:** `/api/v1/anon/in-common-blocklist/<handle/did>`
- **Method:** `GET`
- **Description:** Takes the list of users that a specified user blocks and returns a list of users that also block those same users based on percentage matched
- **Parameters:** handle or did
- **Response:**
    ```json

## 17.

- **Endpoint:** `/api/v1/anon/base/internal/status/process-status`
- **Method:** `GET`
- **Description:** Internal status of server
- **Parameters:** None
- **Response:**
    ```json

## 18.

- **Endpoint:** `/api/v1/anon/in-common-blocked-by/<handle/did>`
- **Method:** `GET`
- **Description:** Takes the list of users that a specified user is blocked by and returns a list of users that also are blocked by those same users based on percentage matched
- **Parameters:** handle or did
- **Response:**
    ```json

## 19.

- **Endpoint:** `/api/v1/anon/lists/dids-per-pds`
- **Method:** `GET`
- **Description:** Return a list of the count of users on each PDS
- **Parameters:** None
- **Response:**
    ```json

## 20.

- **Endpoint:** `/api/v1/anon/validation/validate-handle/<handle>`
- **Method:** `GET`
- **Description:** Validate a handle
- **Parameters:** handle
- **Response:**
    ```json

## 21.

- **Endpoint:** `/api/v1/anon/subscribe-blocks-blocklist/<handle/did>/<page:int>`
- **Method:** `GET`
- **Description:** Get list of lists that a user is blocking
- **Parameters:** handle or did
- **Response:**
    ```json

## 22.

- **Endpoint:** `/api/v1/anon/subscribe-blocks-single-blocklist/<handle/did>/<page:int>`
- **Method:** `GET`
- **Description:** Get list of lists that a user is blocked on
- **Parameters:** handle or did
- **Response:**
    ```json