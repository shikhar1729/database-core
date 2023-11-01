
#include <iostream>
#include<bits/stdc++.h>
#include "tutorial/tutorial.h"
#include "tutorial/trie.h"
#include <cctype>

namespace buzzdb {
namespace tutorial {

#define UNUSED(p) ((void)(p))


/// Your implementation go here
CommandExecutor::CommandExecutor() {}

bool isWordLegal(std::string word)
{
    for(char c : word)
    {
        if(!(std::isalnum(c) || c == '\''))
            return false;
    }
    return true;
}

bool isInteger(std::string str)
{
    return !str.empty() && std::all_of(str.begin(), str.end(), ::isdigit) && std::stoi(str) > 0;

}

std::string truncateString(const std::string& input) {
    auto it = std::find_if_not(input.begin(), input.end(), [](char c) {
        return std::isalnum(c) || c == '\'';
    });
    return std::string(input.begin(), it);
}


buzzdb::tutorial::Trie* head = new Trie();

/// Validate a command
/// Return true/false
std::string CommandExecutor::isValid(std::string command) {
    if(command.length() != 0)
    {
        std::istringstream iss(command);
        std::vector<std::string> tokenizedCmd;
        std::string word;

        while(iss >> word)
            tokenizedCmd.push_back(word);
        

        if((strcasecmp(tokenizedCmd[0].c_str(), "load") == 0) && tokenizedCmd.size() == 2)
        {
            
            std::ifstream file;
            file.open(tokenizedCmd[1]);

            if(file.is_open())
            {                
                std::string word;
                int wordIdx = 1;
                while(file >> word)
                {
                    std::string wordToInsert = truncateString(word);

                    std::transform(wordToInsert.begin(), wordToInsert.end(), wordToInsert.begin(), ::tolower);
                    
                    head->insert(wordToInsert, wordIdx++);
                }
                
                return "";
            }
            
        }
        else if(strcasecmp(tokenizedCmd[0].c_str(), "locate") == 0 && tokenizedCmd.size() == 3)
        {
            if(isWordLegal(tokenizedCmd[1]) && isInteger(tokenizedCmd[2]))
            {
                std::string wordToSearch = tokenizedCmd[1];
                std::transform(wordToSearch.begin(), wordToSearch.end(), wordToSearch.begin(), ::tolower);                
                
                return head->search(wordToSearch, std::stoi(tokenizedCmd[2]));
            }
            
        }
        else if(strcasecmp(tokenizedCmd[0].c_str(), "new") == 0 && tokenizedCmd.size() == 1)
        {
            head->~Trie();            
            return "";
        }
        else if(strcasecmp(tokenizedCmd[0].c_str(), "end") == 0 && tokenizedCmd.size() == 1)
        {
            std::exit(0);

        }
        else
        {
            return "ERROR: Invalid command";

        }
    }

    return "ERROR: Invalid command";
}

/// Execute a command
/// Return the appropriate output as a string
std::string CommandExecutor::execute(std::string command){
    return isValid(command);
}

}  // namespace tutorial
}  // namespace buzzdb
