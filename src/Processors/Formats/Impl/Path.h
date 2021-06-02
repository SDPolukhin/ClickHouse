#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
enum class Type : char
{
    name,
    filter,
    any
};

struct Token
{
    Type type;
    String string_value;
    Token(Type type_, String str) : type(type_), string_value(str) {}
};

class Path
{
public:
    Path(ReadBuffer & in_);
    bool pathMatch(StringRef name_ref);
    bool checkFilter(StringRef name_ref);
    bool advance(ReadBuffer & in);
    void retract();
    size_t current_token;
    ~Path();
private:
    bool advanceToNextToken(size_t token_index, ReadBuffer & in_);
    String readTokenName(ReadBuffer & buf);
    String readFilter(ReadBuffer & buf);
    std::vector<Token> path;
};
}
