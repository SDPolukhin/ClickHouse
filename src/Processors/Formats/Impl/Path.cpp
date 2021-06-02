#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/Path.h>

namespace DB
{
bool Path::advanceToNextToken(size_t token_index, ReadBuffer & in_)
{
    if (in_.eof())
        throw ParsingException("Unexpected end of stream while parsing JSONReadFiltered format", ErrorCodes::CANNOT_READ_ALL_DATA);
    else if (*in_.position() == '{')
    {
        return false;
    }

    if (token_index > 0)
    {
        if (*in_.position() == '.')
        {
            ++in_.position();
            return true;
        }
        else
        {
            skipWhitespaceIfAny(in_);
            if (*in_.position() == '{')
                return false;
            else if (*in_.position() == '?')
            {
                return true;
            }
            else
                throw Exception("Path format error while parsing JSONReadFiltered", ErrorCodes::INCORRECT_DATA);
        }
    }
    return true;
}
String Path::readTokenName(ReadBuffer & in_)
{
    if (!in_.eof() && in_.position() + 1 < in_.buffer().end())
    {
        char * next_pos = find_first_symbols<'"'>(in_.position() + 1, in_.buffer().end());
        if (next_pos != in_.buffer().end())
        {
            assertChar('"', in_);
            String res(in_.position(), next_pos - in_.position());
            in_.position() = next_pos + 1;
            return res;
        }
        else
            throw Exception("Path format error while parsing JSONReadFiltered", ErrorCodes::INCORRECT_DATA);
    }
    else
        throw Exception("Path format error while parsing JSONReadFiltered", ErrorCodes::INCORRECT_DATA);
}
String Path::readFilter(ReadBuffer & in_)
{
    assertChar('?', in_);
    skipWhitespaceIfAny(in_);
    assertChar('(', in_);
    StringRef type(in_.position(), 6);
    if (type == "exists")
    {
        in_.position() += 6;
        skipWhitespaceIfAny(in_);
        assertChar('(', in_);
        skipWhitespaceIfAny(in_);
        assertChar('@', in_);
        assertChar('.', in_);
        char * next_pos = find_first_symbols<'"'>(in_.position() + 1, in_.buffer().end());
        if (next_pos != in_.buffer().end())
        {
            assertChar('"', in_);
            String res(in_.position(), next_pos - in_.position());
            in_.position() = next_pos + 1;
            assertChar(')', in_);
            assertChar(')', in_);
            skipWhitespaceIfAny(in_);
            return res;
        }
        else
            throw Exception("Path format error while parsing JSONReadFiltered", ErrorCodes::INCORRECT_DATA);
    }
    else
    {
        throw Exception("Invalid filter", ErrorCodes::INCORRECT_DATA);
    }
}
Path::Path(ReadBuffer & in_)
{
    path.reserve(10);
    skipWhitespaceIfAny(in_);
    for (size_t token_index = 0; advanceToNextToken(token_index, in_); ++token_index)
    {
        if (*in_.position() == '"')
        {
            path.emplace_back(Type::name, readTokenName(in_));
        }
        else if (*in_.position() == '*')
        {
            assertChar('*', in_);
            path.emplace_back(Type::any, "");
        }
        else
        {
            path.emplace_back(Type::filter, readFilter(in_));
        }
    }
    current_token = 0;
}
bool Path::advance(ReadBuffer & in)
{
    size_t backup = current_token;
    if (current_token + 1 < path.size())
    {
        ++current_token;
        while (path[current_token].type == Type::filter)
        {
            char* scout = in.position();
            bool found = false;
            while (*scout != '}' && !found)
            {
                ++scout;
                if (*scout == '"')
                {
                    ++scout;
                    char* next_pos = find_first_symbols<'"'>(scout, in.buffer().end());
                    StringRef check(scout, next_pos - scout);
                    scout = next_pos;
                    char* nearest_colon = find_first_symbols<':'>(scout, in.buffer().end());
                    char* nearest_end = find_first_symbols<',', '}'>(scout, in.buffer().end());
                    if ((check.toString() == path[current_token].string_value) &&
                    nearest_colon < nearest_end)
                        found = true;
                }
            }
            if (found)
            {
                ++current_token;
                return true;
            }
            else
            {
                current_token = backup;
                return false;
            }
        }
        return true;
    }
    else
    {
        current_token = backup;
        return false;
    }
}
void Path::retract()
{
    if (current_token > 0)
    {
        --current_token;
        while (path[current_token].type == Type::filter && current_token > 0)
            --current_token;
    }
}
bool Path::pathMatch(StringRef name_ref)
{
    switch (path[current_token].type)
    {
        case Type::any:
            return true;
        case Type::name:
            return (path[current_token].string_value == name_ref.toString());
        case Type::filter:
            return false;
    }
    return false;
}
Path::~Path()
{
    path.clear();
}
}