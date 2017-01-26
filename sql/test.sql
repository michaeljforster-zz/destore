-- test.sql

-- The MIT License (MIT)
--
-- Copyright (c) 2016 Michael J. Forster
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.

DELETE FROM destore.dsnapshot;
DELETE FROM destore.devent;
DELETE FROM destore.dstream;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 0,
       '28780637-3dc9-41d9-aec2-a644628f58b1'::uuid, 'com.example.test.cart-created', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": null, "catalogue-name": "Books"}');
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.write_dsnapshot ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, 0,
       '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "BOGO", "catalogue-name": "Books", "items" : ["Product 2"], "payment-type": "Credit Card", "transauth": "ABC666"}');
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.create_dstream ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, 'com.example.test.cart');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 0,
       '28780637-3dc9-41d9-aec2-a644628f58b1'::uuid, 'com.example.test.cart-created', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": null, "catalogue-name": "Books"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 1,
       '81626a0d-760b-44a7-99f0-a8cf03d0f09d'::uuid, 'com.example.test.cart-promotion-applied', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "FREE BOOK"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 2,
       '341822d6-420e-488c-965c-ad4caf137d27'::uuid, 'com.example.test.cart-promotion-applied', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "2 FOR 1"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 3,
       '499bd140-23c2-4834-a241-b3954d4949da'::uuid, 'com.example.test.cart-item-added', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "item": "Product 1"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 4,
       '93f2766a-77eb-4294-a9c2-ef93b7f22530'::uuid, 'com.example.test.cart-item-added', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "item": "Product 2"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 5,
       '5395bb9c-a85e-4233-b756-1c48f7743d36'::uuid, 'com.example.test.cart-item-deleted', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "item": "Product 1"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 6,
       '4b4d2fc5-8c11-49ca-9f4f-8699a0a5daa4'::uuid, 'com.example.test.cart-payment-submitted', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "payment-type": "Credit Card"}');
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 7,
       '7f55ccd0-41e9-49b7-88a5-1a640282308f'::uuid, 'com.example.test.cart-payment-accepted', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "transauth": "ABC666"}');
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 1,
       '81626a0d-760b-44a7-99f0-a8cf03d0f09d'::uuid, 'com.example.test.cart-promotion-applied', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "BOGO"}');
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 8,
       '81626a0d-760b-44a7-99f0-a8cf03d0f09d'::uuid, 'com.example.test.cart-promotion-applied', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "BOGO"}');
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.write_devent ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, NULL, 8,
       '03dd145c-d021-4753-b1fb-a78387811c5c'::uuid, 'com.example.test.cart-promotion-applied', '{}', '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "BOGO"}');
COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT destore.write_dsnapshot ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, 9,
       '{"cart-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "promotion-code": "BOGO", "catalogue-name": "Books", "items" : ["Product 2"], "payment-type": "Credit Card", "transauth": "ABC666"}');
COMMIT;

SELECT * FROM destore.list_all_dstreams();
SELECT * FROM destore.list_all_devents();
SELECT * FROM destore.read_devents('6ae72589-a908-46ec-b12d-83197c669e4c', 0);
SELECT * FROM destore.read_last_dsnapshot('6ae72589-a908-46ec-b12d-83197c669e4c');
