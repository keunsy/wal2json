
Thanks to
============
* eulerto

Forded from
============
* https://github.com/eulerto/wal2json

Reference
=================
* https://www.postgresql.org/docs/9.6/static/logicaldecoding-output-plugin.html

Main Change
=================
* Remove get data from new tuple(when one commit contain too much changes , it will cause performance problem)
* just keep old_keys(primary key) and other basic info
* json example:
{
  "timestamp": "2018-04-11 02:38:16.652863+00",
  "change": [
    {
      "kind": "update",
      "table": "mt_data",
      "oldkeys": {
        "keynames": [
          "_id"
        ],
        "keyvalues": [
          "590db6c53db71d887b906b5c"
        ]
      }
    }
  ]
}
