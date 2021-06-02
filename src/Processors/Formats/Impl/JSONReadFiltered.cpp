#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/JSONReadFiltered.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}
namespace
{
    enum
    {
        UNKNOWN_FIELD = size_t(-1),
        NESTED_FIELD = size_t(-2)
    };
}
JSONReadFiltered::JSONReadFiltered(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_, bool yield_strings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , format_settings(format_settings_)
    , name_map(header_.columns())
    , yield_strings(yield_strings_)
{
    size_t num_columns = getPort().getHeader().columns();
    for (size_t i = 0; i < num_columns; ++i)
    {
        const String & column_name = columnName(i);
        name_map[column_name] = i;
    }
    prev_positions.resize(num_columns);
}
const String & JSONReadFiltered::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}
inline size_t JSONReadFiltered::columnIndex(const StringRef & name, size_t key_index)
{
    if (prev_positions.size() > key_index && prev_positions[key_index] && name == prev_positions[key_index]->getKey())
    {
        return prev_positions[key_index]->getMapped();
    }
    else
    {
        auto * it = name_map.find(name);
        if (it)
        {
            if (key_index < prev_positions.size())
                prev_positions[key_index] = it;
            return it->getMapped();
        }
        else
            return UNKNOWN_FIELD;
    }
}
StringRef JSONReadFiltered::readColumnName(ReadBuffer & buf)
{
    if (!buf.eof() && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());
        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            assertChar('"', buf);
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }
    current_column_name.resize(0);
    readJSONStringInto(current_column_name, buf);
    return current_column_name;
}
static inline void skipColonDelimeter(ReadBuffer & istr)
{
    skipWhitespaceIfAny(istr);
    assertChar(':', istr);
    skipWhitespaceIfAny(istr);
}
void JSONReadFiltered::skipUnknownField(const StringRef & name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);
    skipJSONField(in, name_ref);
}
void JSONReadFiltered::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception("Duplicate field found while parsing JSONEachRow format: " + columnName(index), ErrorCodes::INCORRECT_DATA);

    try
    {
        seen_columns[index] = read_columns[index] = true;
        const auto & type = getPort().getHeader().getByPosition(index).type;
        const auto & serialization = serializations[index];

        if (yield_strings)
        {
            String str;
            readJSONString(str, in);

            ReadBufferFromString buf(str);

            if (format_settings.null_as_default && !type->isNullable())
                read_columns[index] = SerializationNullable::deserializeWholeTextImpl(*columns[index], buf, format_settings, serialization);
            else
                serialization->deserializeWholeText(*columns[index], buf, format_settings);
        }
        else
        {
            if (format_settings.null_as_default && !type->isNullable())
                read_columns[index] = SerializationNullable::deserializeTextJSONImpl(*columns[index], in, format_settings, serialization);
            else
                serialization->deserializeTextJSON(*columns[index], in, format_settings);
        }
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading the value of key " + columnName(index) + ")");
        throw;
    }
}

inline bool JSONReadFiltered::advanceToNextKey(size_t key_index)
{
    skipWhitespaceIfAny(in);
    if (in.eof())
        throw ParsingException("Unexpected end of stream while parsing JSONReadFiltered format", ErrorCodes::CANNOT_READ_ALL_DATA);
    else if (*in.position() == '}')
    {
        ++in.position();
        return false;
    }
    if (key_index > 0)
    {
        assertChar(',', in);
        skipWhitespaceIfAny(in);
    }
    return true;
}
void JSONReadFiltered::readPath()
{
    skipWhitespaceIfAny(in);
    if (read_path == NULL)
        read_path = new Path(in);
    skipWhitespaceIfAny(in);
}
void JSONReadFiltered::readJSONObject(MutableColumns & columns)
{
    assertChar('{', in);
    for (size_t key_index = 0; advanceToNextKey(key_index); ++key_index)
    {
        StringRef name_ref = readColumnName(in);
        const size_t column_index = columnIndex(name_ref, key_index);
        if (read_path->pathMatch(name_ref))
        {
            if (unlikely(ssize_t(column_index) < 0))
            {
                current_column_name.assign(name_ref.data, name_ref.size);
                name_ref = StringRef(current_column_name);
                skipColonDelimeter(in);
                if (*in.position() == '{')
                {
                    if (read_path->advance(in))
                        readJSONObject(columns);
                    else
                        skipJSONField(in, name_ref);
                }
                else if (column_index == UNKNOWN_FIELD)
                    skipUnknownField(name_ref);
                else
                    throw Exception("Logical error: illegal value of column_index", ErrorCodes::LOGICAL_ERROR);
            }
            else
            {
                skipColonDelimeter(in);
                readField(column_index, columns);
            }
        }
        else
        {
            skipColonDelimeter(in);
            skipUnknownField(name_ref);
        }
    }
    read_path->retract();
}
bool JSONReadFiltered::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!allow_new_rows)
        return false;
    skipWhitespaceIfAny(in);
    bool is_first_row = getCurrentUnitNumber() == 0 && getTotalRows() == 1;
    if (!in.eof())
    {
        if (!is_first_row && *in.position() == ',')
            ++in.position();
        else if (!data_in_square_brackets && *in.position() == ';')
        {
            return allow_new_rows = false;
        }
        else if (data_in_square_brackets && *in.position() == ']')
        {
            return allow_new_rows = false;
        }
    }
    skipWhitespaceIfAny(in);
    if (in.eof())
        return false;
    size_t num_columns = columns.size();
    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);
    readPath();
    readJSONObject(columns);
    const auto & header = getPort().getHeader();
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);
    ext.read_columns = read_columns;
    return true;
}
void JSONReadFiltered::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(in);
}
void JSONReadFiltered::resetParser()
{
    IRowInputFormat::resetParser();
    read_columns.clear();
    seen_columns.clear();
    prev_positions.clear();
}
void JSONReadFiltered::readPrefix()
{
    skipBOMIfExists(in);
    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == '[')
    {
        ++in.position();
        data_in_square_brackets = true;
    }
}
void JSONReadFiltered::readSuffix()
{
    skipWhitespaceIfAny(in);
    if (data_in_square_brackets)
    {
        assertChar(']', in);
        skipWhitespaceIfAny(in);
    }
    if (!in.eof() && *in.position() == ';')
    {
        ++in.position();
        skipWhitespaceIfAny(in);
    }
    assertEOF(in);
}
void registerInputFormatProcessorJSONReadFiltered(FormatFactory & factory)
{
    factory.registerInputFormatProcessor(
        "JSONReadFiltered", [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings) {
            return std::make_shared<JSONReadFiltered>(buf, sample, std::move(params), settings, false);
        });
}
JSONReadFiltered::~JSONReadFiltered()
{
    delete read_path;
}
}