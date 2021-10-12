#include <wiredtiger.h>
#include <string>
#include <assert.h>

using namespace std;

namespace WT {
    class Status {
    private:
        bool _ok;
        friend class Iterator;
    public:
        Status(bool _ok = false) : _ok(_ok) {}
        bool ok() {
            return _ok;
        }
    };
    class Slice {
    private:
        string str;
    public:
        Slice(const string &str) : str(str) {}
        string ToString() {
            return str;
        }
    };
    class Options {
    public:
        char * options;
    };
    class DB {
    private:
        WT_CONNECTION *conn = nullptr;
        friend class DBSession;
    public:
        static Status Open(Options &options, const string &home, DB **dbptr) {
            *dbptr = new DB();
            if (*dbptr == nullptr)
                return Status(false);
            if (wiredtiger_open(home.c_str(), NULL, options.options, &(*dbptr)->conn) != 0) {
                delete *dbptr;
                return Status(false);
            }
            return Status(true);
        }
        ~DB() {
            conn->close(conn, NULL);
        }
    };
    class Iterator;
    class DBSession {
    private:
        WT_SESSION *session = nullptr;
        WT_CURSOR *cursor = nullptr;
        string _name;
        friend class Iterator;
    public:
        DBSession(DB *db, const string &name = "map") {
            _name = ("table:" + name);
            db->conn->open_session(db->conn, NULL, NULL, &session);
            session->create(session, _name.c_str(), "key_format=S,value_format=S,checksum=off,"
                            "internal_page_max=4MB,"
                            "leaf_page_max=4MB,"
                            "memory_page_max=2GB,"
                            "split_pct=70"
            );
            session->open_cursor(session, _name.c_str(), NULL, NULL, &cursor);
        }
        ~DBSession() {
            cursor->close(cursor);
            session->close(session, NULL);
        }
        Status Put(Options &options, const string &key, const string &val) {
            cursor->set_key(cursor, key.c_str());
            cursor->set_value(cursor, val.c_str());
            int res = cursor->insert(cursor);
            return Status(res == 0);
        }
        Status Get(Options &options, const string &key, string *val) {
            char *value;
            cursor->set_key(cursor, key.c_str());
            if (cursor->search(cursor) != 0) {
                return Status(false);
            }
            if (cursor->get_value(cursor, &value) != 0) {
                return Status(false);
            }
            *val = string(value);
            return Status(true);
        }
        Status Delete(Options &options, const string &key) {
            cursor->set_key(cursor, key.c_str());
            int res = cursor->remove(cursor);
            return Status(res == 0);
        }
        Iterator *NewIterator(Options &options);
    };
    class Iterator {
    private:
        WT_CURSOR *cursor = nullptr;
        Status s;
    public:
        Iterator(DBSession *session) {
            assert(
                session->session->open_cursor(session->session, session->_name.c_str(), NULL, NULL, &cursor)
             == 0);
        }
        ~Iterator() {
            cursor->close(cursor);
        }
        void SeekToFirst() {
            if (cursor->reset(cursor) != 0) {
                s._ok = false;
                return;
            }
            s._ok = (cursor->next(cursor) == 0);
        }
        void SeekForPrev(const string &key) {
            cursor->set_key(cursor, key.c_str());
            int exact;
            if (cursor->search_near(cursor, &exact) != 0) {
                s._ok = false;
                return;
            }
            if (exact > 0) {
                // get greater key
                cursor->prev(cursor);
            }
        }
        bool Valid() {
            return s._ok;
        }
        void Next() {
            s._ok = (cursor->next(cursor) == 0);
        }
        Slice key() {
            const char *key;
            s._ok = (cursor->get_key(cursor, &key) == 0);
            return Slice(key);
        }
        Slice value() {
            const char *value;
            s._ok = (cursor->get_value(cursor, &value) == 0);
            return Slice(value);
        }
    };

    Iterator *DBSession::NewIterator(Options &options)
    {
        return new Iterator(this);
    }
}