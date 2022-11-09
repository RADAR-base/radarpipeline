from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

ENCODING = "utf-8"
LINESEP = "\n"
CSV_DELIMITER = ","

STRING_TYPE = StringType()
BOOLEAN_TYPE = BooleanType()
BYTE_TYPE = ByteType()
SHORT_TYPE = ShortType()
INT_TYPE = IntegerType()
LONG_TYPE = LongType()
FLOAT_TYPE = FloatType()
DOUBLE_TYPE = DoubleType()

DATA_TYPE_MAPPING = {
    "byte": BYTE_TYPE,
    "short": SHORT_TYPE,
    "int": INT_TYPE,
    "long": LONG_TYPE,
    "float": FLOAT_TYPE,
    "double": DOUBLE_TYPE,
    "string": STRING_TYPE,
    "char": STRING_TYPE,
    "enum": STRING_TYPE,
    "array": STRING_TYPE,
    "boolean": BOOLEAN_TYPE,
}

INTEGER_TYPES = set([BYTE_TYPE, SHORT_TYPE, INT_TYPE, LONG_TYPE])
FLOATING_TYPES = set([FLOAT_TYPE, DOUBLE_TYPE])
