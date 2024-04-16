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
        {
            "data":
                {
                    "lists":
                        [
                            {"created_date":"2023-11-10T21:06:44.219000+00:00","description":"","handle":"vulpido.bsky.social","list count":11,"name":"developers","status":true,"url":"https://bsky.app/profile/did:plc:dowd32x5rh3nqqkqeseayutn/lists/3kduetbtkut2f"},
                            {"created_date":"2023-12-29T11:34:03.625000+00:00","description":"and designers","handle":"yukotan.bsky.social","list count":55,"name":"developers","status":true,"url":"https://bsky.app/profile/did:plc:qexgypv67x75g7bjrelr5gof/lists/3kholoh6fa32m"},
                            {"created_date":"2024-02-09T19:31:00.653000+00:00","description":"","handle":"nklocal.bsky.social","list count":3,"name":"BlueskyDeveloper","status":true,"url":"https://bsky.app/profile/did:plc:wkqs6vwhryiinw35pcfwmrfg/lists/3kmmamibzcu2y"},
                            {"created_date":"2024-02-11T18:24:43.639000+00:00","description":null,"handle":"blaisealicki.bsky.social","list count":11,"name":"Developers","status":true,"url":"https://bsky.app/profile/did:plc:sbigafqky4xbshjw33qg6thx/lists/3kl5x5b566q2a"},
                            ...
                        ],
                    "pages":7
                },
            "input":"dev"
        }

## 11.

- **Endpoint:** `/api/v1/anon/base/autocomplete/<handle:string>`
- **Method:** `GET`
- **Description:** Get a list of handles based on a partial word search
- **Parameters:** handle (partial/complete)
- **Response:**
    ```json
        {
            "suggestions":
                [
                    "thiefchef.bsky.social",
                    "thieftaker.bsky.social",
                    "thieftanja.bsky.social",
                    "thiefhoony.bsky.social",
                    "thiefofvoices.bsky.social"
                ]
        }

## 12.

- **Endpoint:** `/api/v1/anon/total-users`
- **Method:** `GET`
- **Description:** Get user count information: total users, active users, deleted users
- **Parameters:** none
- **Response:**
    ```json
        {
            "data":
                {
                    "active_count":
                        {
                            "displayname":"Active Users","value":"5,506,791"
                        },
                    "deleted_count":
                        {
                            "displayname":"Deleted Users","value":"394,545"
                        },
                    "total_count":
                        {
                            "displayname":"Total Users","value":"5,901,336"
                        }
                }
        }

## 13.

- **Endpoint:** `/api/v1/anon/lists/fun-facts`
- **Method:** `GET`
- **Description:** Get top 20 blockers and blocked
- **Parameters:** None
- **Response:**
    ```json
        {
            "as of":"2024-04-15T12:59:08.467076",
                "data":
                    {
                        "blocked":
                            [
                                {"Handle":"jordanbpeterson.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:aeuetvb7vac3xay76nkphbks","block_count":19376,"did":"did:plc:aeuetvb7vac3xay76nkphbks"},
                                {"Handle":"aifyco.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:s3kgjpyoec2fd7ztruqiaxwx","block_count":15526,"did":"did:plc:s3kgjpyoec2fd7ztruqiaxwx"},
                                {"Handle":"shortcovid.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:rjlu6npi554qkz2jcvdt7mc3","block_count":13744,"did":"did:plc:rjlu6npi554qkz2jcvdt7mc3"},
                                {"Handle":"endwokeness.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:byet53dwfmr5at7xk56zwmxv","block_count":13412,"did":"did:plc:byet53dwfmr5at7xk56zwmxv"},
                                {"Handle":"mdbreathe.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:cmo3ypyyvybcgyi2tg2x4sge","block_count":10384,"did":"did:plc:cmo3ypyyvybcgyi2tg2x4sge"},
                                {"Handle":"90sanime.pics","ProfileURL":"https://bsky.app/profile/did:plc:5krm4pb5gecb5uawvgr7uxuu","block_count":10194,"did":"did:plc:5krm4pb5gecb5uawvgr7uxuu"},
                                {"Handle":"anonymous.expectus.fyi","ProfileURL":"https://bsky.app/profile/did:plc:xfqcsi7wuwedeqaa5m7aih44","block_count":9990,"did":"did:plc:xfqcsi7wuwedeqaa5m7aih44"},
                                {"Handle":"pobachan.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:n6hm45eke7a5qtlqxosdlfus","block_count":9583,"did":"did:plc:n6hm45eke7a5qtlqxosdlfus"},
                                {"Handle":"catswithaura.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:wwakmfq74pvducx2pbbdxhlg","block_count":9030,"did":"did:plc:wwakmfq74pvducx2pbbdxhlg"},
                                {"Handle":"afdberlin.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:vzakzcxo3dqgjvlgmqlakcb5","block_count":8868,"did":"did:plc:vzakzcxo3dqgjvlgmqlakcb5"},
                                {"Handle":"thedevil.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:k2weqoffrljoi7d45jjhjaqk","block_count":8726,"did":"did:plc:k2weqoffrljoi7d45jjhjaqk"},
                                {"Handle":"9gag.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:56eikwlvdd4htq727624spkg","block_count":8444,"did":"did:plc:56eikwlvdd4htq727624spkg"},
                                {"Handle":"nowbreezing.ntw.app","ProfileURL":"https://bsky.app/profile/did:plc:mcb6n67plnrlx4lg35natk2b","block_count":8267,"did":"did:plc:mcb6n67plnrlx4lg35natk2b"},
                                {"Handle":"womensart1.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:jfpub26kfrtponcqzi7i7udl","block_count":7973,"did":"did:plc:jfpub26kfrtponcqzi7i7udl"},
                                {"Handle":"ewerickson.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:2gwoe546lp565vxmzzy2emsl","block_count":7815,"did":"did:plc:2gwoe546lp565vxmzzy2emsl"},
                                {"Handle":"artificial.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:gr3zsqab63cfvv7nao4mrh7v","block_count":7237,"did":"did:plc:gr3zsqab63cfvv7nao4mrh7v"},
                                {"Handle":"poeticalphotos.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:ha5tvry6kkxiujam6kyjgagg","block_count":7171,"did":"did:plc:ha5tvry6kkxiujam6kyjgagg"},
                                {"Handle":"usmc.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:gl5rxplcsmkdygh4w7jjjugd","block_count":6949,"did":"did:plc:gl5rxplcsmkdygh4w7jjjugd"},
                                {"Handle":"julianreichelt.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:hwbjiajbxxij27fzdxwgp7h7","block_count":6905,"did":"did:plc:hwbjiajbxxij27fzdxwgp7h7"},
                                {"Handle":"funnyordie.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:m4wkkbc4k5e46hus7zy3tijd","block_count":6767,"did":"did:plc:m4wkkbc4k5e46hus7zy3tijd"}
                            ],
                        "blocked_aid":
                            {
                                "did:plc:2gwoe546lp565vxmzzy2emsl":"bafkreigkyri2ge24a2gjvbyfqhpv72ean3kqdr5im6e3njdgfcosgtaly4",
                                "did:plc:56eikwlvdd4htq727624spkg":"bafkreic2xkbo646nxl44z3zk2zeighlvsgltoqjoolc3ihc26p4sdxityu",
                                "did:plc:5krm4pb5gecb5uawvgr7uxuu":"bafkreie6vz5gykxxdf777yiirarw3eyvycbg46to6dknynbxwqrcsl5mfu",
                                "did:plc:aeuetvb7vac3xay76nkphbks":"bafkreibnus2odyijekzkiarrthjwwjjaxbqor5ueopfsa3ezk3doyd3t2a",
                                "did:plc:byet53dwfmr5at7xk56zwmxv":"bafkreie5wkm74v6kewoeukcla2hdvzfcfsmqvtm5mhnxdvabgjd2fc2jvm",
                                "did:plc:cmo3ypyyvybcgyi2tg2x4sge":"bafkreicvorlfinnmygp5ibd65wjvr6tq526fkaksgsrls7v4bw3caluko4",
                                "did:plc:gl5rxplcsmkdygh4w7jjjugd":"bafkreigen3wdgs3dyjdvryjrhsks26loqntomm3izgoxfppzk27h64hyta",
                                "did:plc:gr3zsqab63cfvv7nao4mrh7v":"bafkreias26vb2aujzaktxnnzgkqkydf3unkdu4qd6l765fdvqbt5xe6hma",
                                "did:plc:ha5tvry6kkxiujam6kyjgagg":"bafkreigdoleeclxuwukhhqwqsftrofuazhqhvab377v3tyk4jazkvn2ixi",
                                "did:plc:hwbjiajbxxij27fzdxwgp7h7":"bafkreidsd5niiokjiqkrgi3ko444rne6t2qhaeyvyhvz7ahjgyyih3xzie",
                                "did:plc:jfpub26kfrtponcqzi7i7udl":"bafkreib35vtob7wsqhye4t3tgkoprye5qeqv4fauedcqomfm3yj6oae52i",
                                "did:plc:k2weqoffrljoi7d45jjhjaqk":"bafkreiamm5zsg4fs5gt3qv6ljam57a45ulb2ova5zk2f7sagr4cvowiroq",
                                "did:plc:m4wkkbc4k5e46hus7zy3tijd":"bafkreibxjsxyd452tupsqjw252jjnsaqaooyqdhvrdafsu3rhprtcsktfi",
                                "did:plc:mcb6n67plnrlx4lg35natk2b":"bafkreicnd6h4vh3fggtlvajlxizo2qxp5vc3pt7sbqasinlung2ibuwp5q",
                                "did:plc:n6hm45eke7a5qtlqxosdlfus":"bafkreihpmne2lhgdeqpbu6yo4hbi64oghxc4yrglx37oc7qbmg43bl6lm4",
                                "did:plc:rjlu6npi554qkz2jcvdt7mc3":"bafkreia7nn5zcs7eftzihvaqhcqd2wge2s4xyel2rebpvtk7p4sjwwae3y",
                                "did:plc:s3kgjpyoec2fd7ztruqiaxwx":"",
                                "did:plc:vzakzcxo3dqgjvlgmqlakcb5":"bafkreibcalpe4ceeh27wuu42irlkgb3rm6dcxyk3bqcfe7xnjpxxiqojsy",
                                "did:plc:wwakmfq74pvducx2pbbdxhlg":"bafkreieeqwlz7l4lk6eztvyfnowockjstvksrqsfveb7hk2rv5rkfuihtm",
                                "did:plc:xfqcsi7wuwedeqaa5m7aih44":"bafkreic6auduax3rkmtm6xgaetan5osdm454a4y5wc5ryfcjkg5gnfqhmu"},
                        "blockers":
                            [
                                {"Handle":"jpegxl.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:pz7bbehnhhfl2y43kvgsc47j","block_count":324147,"did":"did:plc:pz7bbehnhhfl2y43kvgsc47j"},
                                {"Handle":"tw1nk.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:2np6uovyvjnbgh4l4en5t44a","block_count":314940,"did":"did:plc:2np6uovyvjnbgh4l4en5t44a"},
                                {"Handle":"chiefkeef.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:e4fsmjcj33khuqi6prspceiy","block_count":291146,"did":"did:plc:e4fsmjcj33khuqi6prspceiy"},
                                {"Handle":"jessiealiceboy.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:kuzwlmess4wyoufryjgmub7e","block_count":182344,"did":"did:plc:kuzwlmess4wyoufryjgmub7e"},
                                {"Handle":"evanforman.com","ProfileURL":"https://bsky.app/profile/did:plc:ph5wyq6qnpvjgzoxzilzxwn7","block_count":41169,"did":"did:plc:ph5wyq6qnpvjgzoxzilzxwn7"},
                                {"Handle":"fresh-newlook.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:6odst3452scxthx7w3v2tiav","block_count":40324,"did":"did:plc:6odst3452scxthx7w3v2tiav"},
                                {"Handle":"thewrittentevs.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:mauk6khnlclkr5k2hnezgmja","block_count":20378,"did":"did:plc:mauk6khnlclkr5k2hnezgmja"},
                                {"Handle":"whydoilikethis.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:g2xshwj4o33b5wzxs3xspfxk","block_count":18774,"did":"did:plc:g2xshwj4o33b5wzxs3xspfxk"},
                                {"Handle":"populuxe.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:cgxxlxuypmkdqdcpioqny5r6","block_count":17277,"did":"did:plc:cgxxlxuypmkdqdcpioqny5r6"},
                                {"Handle":"kuriousgeorge.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:43palulr6mc63o4tt2xrpxum","block_count":16940,"did":"did:plc:43palulr6mc63o4tt2xrpxum"},
                                {"Handle":"art-curator.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:25gjrl4ybzmdxxopfpk47mpy","block_count":15918,"did":"did:plc:25gjrl4ybzmdxxopfpk47mpy"},
                                {"Handle":"lukatv.lol","ProfileURL":"https://bsky.app/profile/did:plc:xdvfcobxq6qs7y2dg4luxiga","block_count":15145,"did":"did:plc:xdvfcobxq6qs7y2dg4luxiga"},
                                {"Handle":"hamandegger.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:wgiw4zvjhfhhubf6dtocjern","block_count":15117,"did":"did:plc:wgiw4zvjhfhhubf6dtocjern"},
                                {"Handle":"jerrybuchko.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:m2sds432e3fp3xk2q5m2f344","block_count":14904,"did":"did:plc:m2sds432e3fp3xk2q5m2f344"},
                                {"Handle":"likeamilkdud.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:d2c3m3xnz4e3x774bq7w43pc","block_count":14513,"did":"did:plc:d2c3m3xnz4e3x774bq7w43pc"},
                                {"Handle":"sunsetsnow.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:axrs65zc6hj3m5axp3fth6pw","block_count":14350,"did":"did:plc:axrs65zc6hj3m5axp3fth6pw"},
                                {"Handle":"eugeniemona.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:rb7uc26lx3kn6dl756grswrj","block_count":11045,"did":"did:plc:rb7uc26lx3kn6dl756grswrj"},
                                {"Handle":"northrax.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:o6dfdlenwvg3rhvvi27ai7sq","block_count":10893,"did":"did:plc:o6dfdlenwvg3rhvvi27ai7sq"},
                                {"Handle":"jrbolt.bsky.social","ProfileURL":"https://bsky.app/profile/did:plc:5dn6hroc3v7i53cz6hpq3zgv","block_count":10599,"did":"did:plc:5dn6hroc3v7i53cz6hpq3zgv"},
                                {"Handle":"articulatemadness.com","ProfileURL":"https://bsky.app/profile/did:plc:if6lbbt7vjduzzfdlgea4m4n","block_count":10598,"did":"did:plc:if6lbbt7vjduzzfdlgea4m4n"}
                            ],
                        "blockers_aid":
                            {
                                "did:plc:25gjrl4ybzmdxxopfpk47mpy":"",
                                "did:plc:2np6uovyvjnbgh4l4en5t44a":"bafkreifrkgduawqxsgvtwdbxuiyf3an6csbafowd6m5lj2wprhlo3ytsom",
                                "did:plc:43palulr6mc63o4tt2xrpxum":"bafkreib3oemett3th4df4joxyrztwi53yajqdfbqsgno5gqcgfucz4kamu",
                                "did:plc:5dn6hroc3v7i53cz6hpq3zgv":"bafkreigiy7jwy7rywt2s7mid6vcksweekxbghk4hwn4kkdamtzyzwzvczi",
                                "did:plc:6odst3452scxthx7w3v2tiav":"bafkreid4znwozgnn5vz7jhk5wsub73dkwnqf3hu4cs7u5njewqiixugi4q",
                                "did:plc:axrs65zc6hj3m5axp3fth6pw":"bafkreicqwvuqqhzi6vzexyi7c5arfov4cqr26hkxggameqmd3nkldovud4",
                                "did:plc:cgxxlxuypmkdqdcpioqny5r6":"bafkreiaaaksgtbfk7wvuwrzgyrssvfqm7gzxmilsf6w4jz33eiz6auwyo4",
                                "did:plc:d2c3m3xnz4e3x774bq7w43pc":"bafkreig672aogrqgj6hj3fhilgetxrfty4pf3vv2q25sjzm4uicnvs3wru",
                                "did:plc:e4fsmjcj33khuqi6prspceiy":"bafkreifykraakgmpblk7q574amv23yffetp43fmxubja4vnhu74rob3ida",
                                "did:plc:g2xshwj4o33b5wzxs3xspfxk":"bafkreiabzguv3uvpth3a3w2fvgwhvl7qzmvcqn63sodk4ds5lp5v3g2y34",
                                "did:plc:if6lbbt7vjduzzfdlgea4m4n":"bafkreidcngprpdc7qqp6f3dak4zjjjakekoaemnp6puyemofd2t4rr3uu4",
                                "did:plc:kuzwlmess4wyoufryjgmub7e":"bafkreih5rrhkaafw57z6ovbktfjdgavmfjt7fvqdjblz444lr4gq3ozrxu",
                                "did:plc:m2sds432e3fp3xk2q5m2f344":"bafkreifvyu5u5zmrr77nf6cpvapj6dfhbbo6ibfhvmmeccanwkghfdfu74",
                                "did:plc:mauk6khnlclkr5k2hnezgmja":"bafkreigbmijkhof4qsutjttodry4h33euthlu6xcvezzxlie2aejuhteaq",
                                "did:plc:o6dfdlenwvg3rhvvi27ai7sq":"bafkreig3gzi2uo26wwf6v52quhd6sci43bzjzkai2jnqzq7vty755dszle",
                                "did:plc:ph5wyq6qnpvjgzoxzilzxwn7":"bafkreifif4aj4lovhiqkxnr5r6iceu4c5evijqqm45jsi5f2u5ajim2syq",
                                "did:plc:pz7bbehnhhfl2y43kvgsc47j":"bafkreiaqgit3wn3rny4mpixcbwedj3v4k76nf6tbal77d5jrqr7vnd34ua",
                                "did:plc:rb7uc26lx3kn6dl756grswrj":"bafkreigahnudz2pbcmymmei54wuomvxemn25ofzakjonjcmwaukdyf3hki",
                                "did:plc:wgiw4zvjhfhhubf6dtocjern":"bafkreibkkfry5co2opt3itgcx3y2dkuqwpy3kzwkmf5b6b7q6i76qutduu",
                                "did:plc:xdvfcobxq6qs7y2dg4luxiga":"bafkreif5mw66ul2xxt2wzlddvljcydfk4wwkpkyyddgucsd3r44vtpv6b4"
                            }
                    }
        }

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
        {
            "as of":"2024-04-16T01:41:27.900623",
                "data":
                    {
                        "averageNumberOfBlocked":{"displayname":"Average Number of Users Blocked","value":5.84},
                        "averageNumberOfBlocks":{"displayname":"Average Number of Blocks","value":"22.09"},
                        "numberBlock1":{"displayname":"Number of Users Blocking 1 User","value":"218,028"},
                        "numberBlocked1":{"displayname":"Number of Users Blocked by 1 User","value":"908,864"},
                        "numberBlocked101and1000":{"displayname":"Number of Users Blocked by 101-1000 Users","value":"9,954"},
                        "numberBlocked2and100":{"displayname":"Number of Users Blocked by 2-100 Users","value":"1,021,503"},
                        "numberBlockedGreaterThan1000":{"displayname":"Number of Users Blocked by More than 1000 Users","value":"880"},
                        "numberBlocking101and1000":{"displayname":"Number of Users Blocking 101-1000 Users","value":"12,653"},
                        "numberBlocking2and100":{"displayname":"Number of Users Blocking 2-100 Users","value":"281,329"},
                        "numberBlockingGreaterThan1000":{"displayname":"Number of Users Blocking More than 1000 Users","value":"751"},
                        "numberOfTotalBlocks":{"displayname":"Number of Total Blocks","value":"11,335,104"},
                        "numberOfUniqueUsersBlocked":{"displayname":"Number of Unique Users Blocked","value":"1,941,201"},
                        "numberOfUniqueUsersBlocking":{"displayname":"Number of Unique Users Blocking","value":"512,761"},
                        "percentNumberBlocked1":{"displayname":"Percent of Users Blocked by 1 User","value":46.82},
                        "percentNumberBlocked101and1000":{"displayname":"Percent of Users Blocked by 101-1000 Users","value":0.51},
                        "percentNumberBlocked2and100":{"displayname":"Percent of Users Blocked by 2-100 Users","value":52.62},
                        "percentNumberBlockedGreaterThan1000":{"displayname":"Percent of Users Blocked by More than 1000 Users","value":0.05},
                        "percentNumberBlocking1":{"displayname":"Percent of Users Blocking 1 User","value":42.52},
                        "percentNumberBlocking101and1000":{"displayname":"Percent of Users Blocking 101-1000 Users","value":2.47},
                        "percentNumberBlocking2and100":{"displayname":"Percent of Users Blocking 2-100 Users","value":54.87},
                        "percentNumberBlockingGreaterThan1000":{"displayname":"Percent of Users Blocking More than 1000 Users","value":0.15},
                        "percentUsersBlocked":{"displayname":"Percent Users Blocked","value":32.86},
                        "percentUsersBlocking":{"displayname":"Percent Users Blocking","value":8.68},
                        "totalUsers":{"displayname":"Total Users","value":"5,906,944"}
                    }
        }

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
        {
            "data":
                {
                    "https://42d.fr":1,"https://465789.xyz":1,
                    "https://80px.org":3,
                    "https://afternooncurry.com":1,
                    "https://agaric.us-west.host.bsky.network":276517,
                    ...
                }
        }

## 20.

- **Endpoint:** `/api/v1/anon/validation/validate-handle/<handle>`
- **Method:** `GET`
- **Description:** Validate a handle
- **Parameters:** handle
- **Response:**
    ```json
        {
            "data":
                {
                    "valid":"true"
                },
            "identity":"thieflord.dev"
        }


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