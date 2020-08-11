#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, logging
logging.basicConfig(level=logging.INFO)
import aiomysql

import sys

def log(sql, args=()):
    logging.info('SQL: %s' %sql)
    
async def create_pool(loop, **kw):
    logging.info('create database connection pool ...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', '3306'),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )
#单独封装select
#insert，update，delete语句一起封装

async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:  #打开pool的方法：with await __pool as conn:
        #创建一个结果为字典的游标
        async with conn.cursor(aiomysql.DictCursor) as cur:
        #创建sql语句,将sql语句中的'?'替换成'%s'
            await cur.execute(sql.replace('?','%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' %len(rs))
        return rs
    
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                #获取操作的记录数
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback() #数据回滚
            raise
        return affected
        
#用于输出元类中创建sql_insert语句中的占位符，计算需要拼接多少个占位符
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)
    
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
        
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
        
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)
        
class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)
        
class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)
        
class IntegerField(Field):


    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)
        
class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)
      
class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)
        
#定义Model的metaclass元类
#所有的元类都继承自type
#ModelMetaclass元类定义了所有Model基类（继承ModlMetaclass）的子类实现的操作

# -*-ModelMetaclass：为一个数据库表映射成一个封装的类做准备
# 读取具体子类（eg：user）的映射信息
#创作类的时候，排除对Model的修改
#在当前类中查找所有的类属性（attrs），如果找到Field属性，就保存在__mappings__的dict里，
#实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法
#__table__保存数据库表名

class ModelMetaclass(type):
    # 调用__init__方法会调用__new__方法
    def __new__(cls, name, bases, attrs):
    # cls:当前准备创建的类的对象，name:类的名称，
    # bases:类继承的父类集合，attrs:类的方法集合
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称，如果未设置，tableName就是类的名字
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' %(name, tableName))
        # 获取所有的Field(类属性)和主键名：
        mappings = dict()         #保存映射关系
        fields = []               #保存主键外的属性
        primaryKey = None
        # key是列名， value是field的子类
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mappings: %s ==> %s' %(k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = k   #在此列设为列表的主键
                else:
                    # 非主键，一律放在fields
                    fields.append(k)
        if not primaryKey:           #如果遍历了所有属性都没有找到主键，则主键没定义
            raise StandardError('Primary key not found.')
        #从类属性中删除Field属性
        for k in mappings.keys():
            attrs.pop(k)             #从类属性中删除Field属性，否则，容易造成运行错误(实例的属性会遮盖类的同名属性)
        
        # 保存非主键属性为字符串列表形式
        # 将非主键属性变成‘id’，‘name’这种形式(带反引号)
        # repr函数和反引号的功能一致：取得对象的规范字符串表示
        # 将fields中属性名以‘属性名’的方式装饰起来
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings      #保存属性和列的映射关系
        attrs['__table__'] = tableName        #保存表名
        attrs['__primary_key__'] = primaryKey #主键属性名
        attrs['__fields__'] = fields          #除主键外的属性名
        
        #构造默认的SELECT, INSERT, UPDATE和DELETE语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields),  tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):
    #也可在此处写 __metaclass__ = ModelMetaclass, 与参数处效果相同
    
    def __init__(self,**kw):
        super(Model, self).__init__(**kw)
        
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%w'" % key)
            
    def __setattrs__(self, key, value):
        self[key] = value
        
    def getValue(self, key):
        #返回对象的属性， 如果没有对应的属性，则会调用__getattr__
        #直接调回内置函数，注意这里没有下划符，注意这里None的用处，是为了当user没有赋值数据时，返回None，调用update
        return getattr(self, key, None)
        
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' %(key, str(value)))
                # 将默认值设置进行
                setattr(self, key, value)
        return value
#类方法第一个参数为cls，而实例方法的第一个参数为self

    @classmethod
        #这里可以使用User.findAll()是因为：@classmethod修饰了Model类里面的findAll()
        #一般来说，要使用某个类的方法，需要先实例化一个对象再调用方法
        #而使用@staticmethod或@classmethod，就可以不需要实例化，直接类名.方法名()来调用
        #申明是类方法：有类变量cls传入，cls可以做一些相关的处理
        #有子类继承时，调用改方法，传入的类变量cls是子类，而非父类
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')  #添加where关键字
            sql.append(where)    #拼接where查询条件
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')  #拼接orderBy字符串
            sql.append(orderBy)     #拼接orderBy查询条件
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int): #如果limit是int型
                sql.append('?')       #sql语句拼接一个占位符
                args.append(limit)    #将limit添加到参数列表，之所以天剑参数列表之后再进行整合是为了防止sql注入
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?,?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
            rs = await select(' '.join(sql), args) #将args参数列表注入sql语句之后，传递给select函数进行查询并返回查询结果
            return [cls(**r) for r in rs]
            
    @classmethod
    #查询某个字段的数量
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        # 将列名重命名为__num
        sql = ['select %s __num__ from ‘%s’' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
            #限制结果数为1
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']
        
    @classmethod

    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s=?`' %(cls.__select__, cls.__primary_key__))
        if len(rs) == 0:
            return None
        return cls(**rs[0])
        #返回一条记录，以dict的形式返回，因为clsde的父类继承了dict类
    
    async def save(self):
        #获取所有的value
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)
           
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
           
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' %rows)
        
