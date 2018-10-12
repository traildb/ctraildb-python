#include <Python.h>
#include <traildb.h>
#include <stdio.h>
#include <stdint.h>

struct sTrailDBEventTypeBase;

typedef struct {
    PyObject_HEAD
    tdb* t;
    uint64_t num_trails;
    uint64_t num_fields;
    PyObject* field_attrs;
} TrailDBObject;

typedef struct {
    PyObject_HEAD
    TrailDBObject* t;
    tdb_cursor* c;
    uint64_t timestamp;
    uint64_t trail_id;
    int exhausted;
} TrailDBCursorObject;

typedef struct {
    PyObject_HEAD
    uint64_t trail_id;
    TrailDBObject* t;
    TrailDBCursorObject* c;
} TrailDBTrailsIterator;

typedef struct sTrailDBEventObject {
    PyObject_HEAD
    TrailDBObject* t;
    uint64_t timestamp;
    uint64_t trail_id;
    tdb_item* items;
} TrailDBEventObject;

typedef struct sTrailDBFiltersAttrsObject {
    PyObject_HEAD
    PyDictObject* attrs;
} TrailDBFieldAttrsObject;

static int
ctraildb_TrailDBCursor(PyObject *self_pobj, PyObject *args, PyObject* kwds);
static PyObject *
ctraildb_iter(PyObject* self_pobj);
static PyObject *
ctraildb_iternext(PyObject* self_pobj);
static PyObject *
ctraildb_TrailDBTrailsIterator_iter(PyObject* self_pobj);
static PyObject *
ctraildb_TrailDBTrailsIterator_iternext(PyObject* self_pobj);
static void
ctraildb_TrailDBTrailsIterator_del(PyObject* self_pobj);
static void
ctraildb_delCursor(PyObject *self_pobj);
static PyObject *
ctraildb_cursor_get_trail(PyObject* self_pobj, PyObject* trail_id_pobj);
static void ctraildb_Event_del(PyObject* self_pobj);
static signed long
load_to_attr_cache(TrailDBEventObject* self, PyObject* attr_name);
static PyObject *
ctraildb_Event_getattro(PyObject* self_pobj, PyObject* attr_name);
static void ctraildb_FieldAttrs_del(PyObject* self);

static PyMethodDef ctraildb_cursor_methods[] = {
    { .ml_name = "get_trail",
      .ml_flags = METH_O,
      .ml_meth = ctraildb_cursor_get_trail,
      .ml_doc = "Set the cursor to a certain trail in the TrailDB"
    },
    NULL,
};

static PyTypeObject TrailDBCursorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ctraildb.TrailDBCursor",
    .tp_doc = "TrailDB cursor",
    .tp_basicsize = sizeof(TrailDBCursorObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_init = ctraildb_TrailDBCursor,
    .tp_dealloc = ctraildb_delCursor,
    .tp_iter = ctraildb_iter,
    .tp_iternext = ctraildb_iternext,
    .tp_methods = ctraildb_cursor_methods,
};

static PyTypeObject TrailDBTrailsIteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ctraildb.TrailDBTrailsIterator",
    .tp_doc = "Iterator over all trails in a TrailDB",
    .tp_basicsize = sizeof(TrailDBTrailsIterator),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = ctraildb_TrailDBTrailsIterator_del,
    .tp_iter = ctraildb_TrailDBTrailsIterator_iter,
    .tp_iternext = ctraildb_TrailDBTrailsIterator_iternext,
};

static PyTypeObject TrailDBEventType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ctraildb.TrailDBEvent",
    .tp_doc = "Event in a TrailDB",
    .tp_basicsize = sizeof(TrailDBEventObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = ctraildb_Event_del,
    .tp_getattro = ctraildb_Event_getattro,
};

static PyTypeObject TrailDBFieldAttrsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ctraildb.TrailDBFieldAttrs",
    .tp_doc = "Dictionary of attribute names to field indices (internal)",
    .tp_basicsize = sizeof(TrailDBFieldAttrsObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = ctraildb_FieldAttrs_del,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_setattro = PyObject_GenericSetAttr,
    .tp_dictoffset = -sizeof(PyDictObject*),
};

static PyObject *
ctraildb_iter(PyObject* self_pobj);

static PyObject *
ctraildb_Event_getattro(PyObject* self_pobj, PyObject* attr_name)
{
    signed long field_id = -1;
    TrailDBEventObject* self = (TrailDBEventObject*) self_pobj;
    PyObject* ret = PyObject_GetAttr(self->t->field_attrs, attr_name);
    if (!ret) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            field_id = load_to_attr_cache(self, attr_name);
            if (field_id == -1) {
                return NULL;
            }
        } else {
            return NULL;
        }
    } else {
        field_id = PyLong_AsLong(ret);
        if (field_id == -1 && PyErr_Occurred()) {
            return NULL;
        }
    }

    switch (field_id) {
        case 0: // uuid
        {
            const uint8_t* uuid = tdb_get_uuid(self->t->t, self->trail_id);
            uint8_t hexuuid[32];
            tdb_uuid_hex(uuid, hexuuid);
            return PyBytes_FromStringAndSize((char*) hexuuid, 32);
        }
        case 1: // time
            return PyLong_FromUnsignedLongLong((unsigned long long) self->timestamp);
        break;
        default:
        {
            field_id -= 2;
            size_t len;
            const char* val = tdb_get_item_value(self->t->t, self->items[field_id], &len);
            return PyBytes_FromStringAndSize(val, len);
        }
    }
    return ret;
}

static signed long
load_to_attr_cache(TrailDBEventObject* self, PyObject* attr_name)
{
#if PY_MAJOR_VERSION >= 3
    const char* attr_name_cstr = PyUnicode_AsUTF8(attr_name);
    if (!attr_name_cstr) {
        return -1;
    }
#else
    const char* attr_name_cstr = PyBytes_AsString(attr_name);
    if (!attr_name_cstr) {
        return -1;
    }
#endif
    if (!strcmp(attr_name_cstr, "uuid")) {
        return 0;
    }
    if (!strcmp(attr_name_cstr, "time")) {
        return 1;
    }
    TrailDBObject* t = self->t;
    for (int i = 0; i < t->num_fields-1; ++i) {
        if (!strcmp(tdb_get_field_name(t->t, (tdb_field) i+1), attr_name_cstr)) {
            PyObject* val = PyLong_FromLong((long) (i+2));
            if (!val) {
                return -1;
            }
            if (PyObject_SetAttr(self->t->field_attrs, attr_name, val) < 0) {
                return -1;
            }
            return (signed long) (i+2);
        }
    }
    char errstr[500];
    snprintf(errstr, 499, "No such field in TrailDB: '%s'", attr_name_cstr);
    errstr[499] = 0;
    PyErr_SetString(PyExc_KeyError, errstr);
    return -1;
}

static void ctraildb_Event_del(PyObject* self_pobj)
{
    TrailDBEventObject* self = (TrailDBEventObject*) self_pobj;
    if (self->items) {
        PyMem_Free(self->items);
        self->items = NULL;
    }
    if (self->t) {
        Py_DECREF(self->t);
        self->t = NULL;
    }
    PyObject_Del(self_pobj);
}

static void ctraildb_FieldAttrs_del(PyObject* self_pobj)
{
    TrailDBFieldAttrsObject* self = (TrailDBFieldAttrsObject*) self_pobj;
    if (self->attrs) {
        Py_DECREF(self->attrs);
    }
    PyObject_Del(self);
}

static void
ctraildb_del(PyObject *self_pobj)
{
    TrailDBObject* self = (TrailDBObject*) self_pobj;
    tdb* t = NULL;
    if (self->t) {
        t = self->t;
        self->t = NULL;
    }
    if (self->field_attrs) {
        Py_DECREF(self->field_attrs);
        self->field_attrs = NULL;
    }
    PyObject_Del(self_pobj);
    tdb_close(t);
}

static int
ctraildb_TrailDB(PyObject *self_pobj, PyObject *args, PyObject* kwds)
{
    static char* kwlist[] = {"path", NULL};
    TrailDBObject* self = (TrailDBObject*) self_pobj;
    const char* traildb_name;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s", kwlist, &traildb_name))
        return -1;

    tdb* t = tdb_init();
    if (!t) {
        PyErr_SetString(PyExc_MemoryError, "Not enough memory to allocate 'tdb' object.");
        return -1;
    }

    tdb_error err = tdb_open(t, traildb_name);
    if (err != TDB_ERR_OK) {
        char errstr[500];
        snprintf(errstr, 499, "Cannot open TrailDB '%s': %s", traildb_name, tdb_error_str(err));
        errstr[499] = 0;
        PyErr_SetString(PyExc_OSError, errstr);
        return -1;
    }

    TrailDBFieldAttrsObject* field_attrs = PyObject_New(TrailDBFieldAttrsObject, (PyTypeObject*) &TrailDBFieldAttrsType);
    if (!field_attrs) {
        tdb_close(t);
        return -1;
    }
    field_attrs->attrs = (PyDictObject*) PyDict_New();
    if (!field_attrs->attrs) {
        Py_DECREF(field_attrs);
        tdb_close(t);
        return -1;
    }

    self->t = t;
    self->num_trails = tdb_num_trails(t);
    self->num_fields = tdb_num_fields(t)+1;
    self->field_attrs = (PyObject*) field_attrs;
    return 0;
}

static PyObject *
ctraildb_num_trails(PyObject* self_pobj, void* dummy)
{
    TrailDBObject* self = (TrailDBObject*) self_pobj;
    if (!self->t) {
        PyErr_SetString(PyExc_ValueError, "Cannot read num_trails: TrailDB has been closed.");
        return NULL;
    }
    uint64_t count = tdb_num_trails(self->t);
    return Py_BuildValue("K", (unsigned long long) count);
}

static PyGetSetDef ctraildb_getsets[] = {
    { .name = "num_trails",
      .get = ctraildb_num_trails },
    NULL
};

static PyObject *
ctraildb_TrailDB_trails(PyObject* self_pobj, PyObject* dummy)
{
    TrailDBObject* self = (TrailDBObject*) self_pobj;
    if (!self->t) {
        PyErr_SetString(PyExc_ValueError, "Cannot read num_trails: TrailDB has been closed.");
        return NULL;
    }
    TrailDBTrailsIterator* pobj = (TrailDBTrailsIterator*) PyObject_New(TrailDBTrailsIterator, (PyTypeObject*) &TrailDBTrailsIteratorType);
    if (!pobj)
        return NULL;
    TrailDBCursorObject* c = (TrailDBCursorObject*) PyObject_New(TrailDBCursorObject, &TrailDBCursorType);
    if (!c) {
        Py_DECREF(pobj);
        return NULL;
    }
    tdb_cursor* cur = tdb_cursor_new(self->t);
    if (!cur) {
        Py_DECREF(pobj);
        Py_DECREF(c);
        return NULL;
    }
    Py_INCREF(self);
    c->t = self;
    c->c = cur;
    c->exhausted = 1;
    c->trail_id = 0;

    pobj->t = self;
    Py_INCREF(self);
    pobj->c = c;
    return (PyObject*) pobj;
}

static PyObject *
ctraildb_TrailDB_get_uuid(PyObject* self_pobj, PyObject* arg)
{
    TrailDBObject* self = (TrailDBObject*) self_pobj;
    signed long trail_id = PyLong_AsLong(arg);
    if (trail_id == -1 && PyErr_Occurred()) {
        return NULL;
    }
    if (trail_id < 0 || trail_id >= self->num_trails) {
        PyErr_SetString(PyExc_ValueError, "Trail ID is outside of range.");
        return NULL;
    }
    const uint8_t* uuid = tdb_get_uuid(self->t, trail_id);
    uint8_t hexuuid[32];
    tdb_uuid_hex(uuid, hexuuid);
    return PyBytes_FromStringAndSize((char*) hexuuid, 32);
}

static PyMethodDef ctraildb_methods[] = {
    { .ml_name = "trails",
      .ml_meth = ctraildb_TrailDB_trails,
      .ml_flags = METH_NOARGS,
      .ml_doc = "Return an iterator over all trails in the TrailDB." },
    { .ml_name = "get_uuid",
      .ml_flags = METH_O,
      .ml_meth = ctraildb_TrailDB_get_uuid,
      .ml_doc = "Get the UUID of some Trail ID." },
    NULL
};

static Py_ssize_t ctraildb_TrailDB_sq_length(PyObject* self_pobj)
{
    TrailDBObject* self = (TrailDBObject*) self_pobj;
    if (!self->t) {
        PyErr_SetString(PyExc_ValueError, "Cannot read num_trails: TrailDB has been closed.");
        return -1;
    }
    return tdb_num_trails(self->t);
}

static PySequenceMethods TrailDBSequenceMethods = {
    .sq_length = ctraildb_TrailDB_sq_length,
};

static PyTypeObject TrailDBObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ctraildb.TrailDB",
    .tp_doc = "TrailDB handle",
    .tp_basicsize = sizeof(TrailDBObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_init = ctraildb_TrailDB,
    .tp_dealloc = ctraildb_del,
    .tp_getset = ctraildb_getsets,
    .tp_methods = ctraildb_methods,
    .tp_as_sequence = &TrailDBSequenceMethods,
};

static PyObject *
ctraildb_TrailDBTrailsIterator_iter(PyObject* self_pobj)
{
    TrailDBTrailsIterator* self = (TrailDBTrailsIterator*) self_pobj;
    self->trail_id = 0;
    self->c->trail_id = 0;
    Py_INCREF(self);
    return (PyObject*) self;
}

static PyObject *
ctraildb_TrailDBTrailsIterator_iternext(PyObject* self_pobj)
{
    TrailDBTrailsIterator* self = (TrailDBTrailsIterator*) self_pobj;
    if (self->trail_id < self->t->num_trails) {
        self->c->trail_id = self->trail_id;
        self->trail_id++;
        const uint8_t* uuid = tdb_get_uuid(self->t->t, self->c->trail_id);
        uint8_t hexuuid[32];
        tdb_uuid_hex(uuid, hexuuid);
#if PY_MAJOR_VERSION >= 3
        return Py_BuildValue("y#O", (char*) hexuuid, 32, (PyObject*) self->c);
#else
        return Py_BuildValue("s#O", (char*) hexuuid, 32, (PyObject*) self->c);
#endif
    }
    return NULL;
}

static void
ctraildb_TrailDBTrailsIterator_del(PyObject* self_pobj)
{
    TrailDBTrailsIterator* self = (TrailDBTrailsIterator*) self_pobj;
    if (self->c) {
        Py_DECREF(self->c);
        self->c = NULL;
    }
    if (self->t) {
        Py_DECREF(self->t);
        self->t = NULL;
    }
    PyObject_Del(self_pobj);
}

static void
ctraildb_delCursor(PyObject *self_pobj)
{
    TrailDBCursorObject* self = (TrailDBCursorObject*) self_pobj;
    if (self->c) {
        tdb_cursor_free(self->c);
        self->c = NULL;
    }
    if (self->t) {
        Py_DECREF(self->t);
        self->t = NULL;
    }
    PyObject_Del(self_pobj);
}

static int
ctraildb_TrailDBCursor(PyObject *self_pobj, PyObject *args, PyObject* kwds)
{
    static char* kwlist[] = {"traildb", NULL};
    TrailDBCursorObject* self = (TrailDBCursorObject*) self_pobj;
    TrailDBObject* tobj;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!", kwlist, &TrailDBObjectType, &tobj))
        return -1;

    tdb_cursor* c = tdb_cursor_new(tobj->t);
    if (!c) {
        PyErr_SetString(PyExc_MemoryError, "Not enough memory to allocate 'TrailDBCursor' object.");
        return -1;
    }

    Py_INCREF(tobj);
    self->t = tobj;
    self->c = c;
    self->exhausted = 1;
    self->trail_id = 0;
    return 0;
}

static PyObject *
ctraildb_iter(PyObject* self_pobj)
{
    TrailDBCursorObject* self = (TrailDBCursorObject*) self_pobj;
    self->exhausted = 0;
    if (self->trail_id >= self->t->num_trails) {
        PyErr_SetString(PyExc_KeyError, "Attempt to iterate a TrailDBCursor beyond allowed trail ID range.");
        return NULL;
    }
    tdb_error err = tdb_get_trail(self->c, self->trail_id);
    if (err != TDB_ERR_OK) {
        char errstr[500];
        snprintf(errstr, 499, "tdb_get_trail() failed: %s", tdb_error_str(err));
        errstr[499] = 0;
        PyErr_SetString(PyExc_OSError, errstr);
        return NULL;
    }
    Py_INCREF(self_pobj);
    return self_pobj;
}

static PyObject *
ctraildb_iternext(PyObject* self_pobj)
{
    TrailDBCursorObject* self = (TrailDBCursorObject*) self_pobj;
    const tdb_event* ev = tdb_cursor_next(self->c);
    if (ev) {
        TrailDBEventObject* evobj = (TrailDBEventObject*) PyObject_New(TrailDBEventObject, &TrailDBEventType);
        if (!evobj) {
            return NULL;
        }
#if PY_MAJOR_VERSION >= 3
        evobj->items = PyMem_Calloc(self->t->num_fields, sizeof(tdb_item));
#else
        evobj->items = PyMem_Malloc(self->t->num_fields * sizeof(tdb_item));
#endif
        if (!evobj->items) {
            Py_DECREF(evobj);
            PyErr_SetString(PyExc_MemoryError, "Cannot allocate TDB event.");
            return NULL;
        }
        memcpy(evobj->items, ev->items, self->t->num_fields*sizeof(tdb_item));
        evobj->trail_id = self->trail_id;
        evobj->timestamp = ev->timestamp;
        evobj->t = self->t;
        Py_INCREF(self->t);
        return (PyObject*) evobj;
    } else {
        return NULL;
    }
}

static PyObject *
ctraildb_cursor_get_trail(PyObject* self_pobj, PyObject* trail_id_pobj)
{
    TrailDBCursorObject* self = (TrailDBCursorObject*) self_pobj;
    long long tid = PyLong_AsLongLong(trail_id_pobj);
    if (tid == -1 && PyErr_Occurred()) {
        return NULL;
    }
    self->trail_id = tid;
    Py_RETURN_NONE;
}

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef ctraildbmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "ctraildb",
    .m_doc = NULL,
    .m_size = -1,
};
#endif

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_ctraildb(void)
#else
PyMODINIT_FUNC
initctraildb(void)
#endif
{
    PyObject* m;
#if PY_MAJOR_VERSION >= 3
    if (PyType_Ready(&TrailDBObjectType) < 0)
        return NULL;
    if (PyType_Ready(&TrailDBCursorType) < 0)
        return NULL;
    if (PyType_Ready(&TrailDBEventType) < 0)
        return NULL;
    if (PyType_Ready(&TrailDBFieldAttrsType) < 0)
        return NULL;
#else
    if (PyType_Ready(&TrailDBObjectType) < 0)
        return;
    if (PyType_Ready(&TrailDBCursorType) < 0)
        return;
    if (PyType_Ready(&TrailDBEventType) < 0)
        return;
    if (PyType_Ready(&TrailDBFieldAttrsType) < 0)
        return;
#endif

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&ctraildbmodule);
    if (m == NULL)
        return NULL;
#else
    m = Py_InitModule3("ctraildb", NULL, NULL);
    if (m == NULL)
        return;
#endif

    Py_INCREF(&TrailDBObjectType);
    Py_INCREF(&TrailDBCursorType);
    Py_INCREF(&TrailDBEventType);
    Py_INCREF(&TrailDBFieldAttrsType);
    PyModule_AddObject(m, "TrailDB", (PyObject*) &TrailDBObjectType);
    PyModule_AddObject(m, "TrailDBCursor", (PyObject*) &TrailDBCursorType);
    PyModule_AddObject(m, "TrailDBEvent", (PyObject*) &TrailDBEventType);
    PyModule_AddObject(m, "TrailDBFieldAttrs", (PyObject*) &TrailDBFieldAttrsType);
#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
