#include <iostream>
#include<bits/stdc++.h>
#include "tutorial/trie.h"

//using namespace std;
 
namespace buzzdb {
namespace tutorial {
// Define the character size

 
// A class to store a Trie node
Trie::Trie()
{
    isLeaf = false; 
    for (std::size_t i = 0; i < CHAR_SIZE; i++) {
        character[i] = nullptr;
    }   
 
    void insert(std::string, int);
    int search(std::string, int);
};

Trie::~Trie()
{
    for (std::size_t i = 0; i < CHAR_SIZE; i++) {
        if (character[i] != nullptr) {
            delete character[i]; // Recursively delete child nodes
            character[i] = nullptr; // Set the pointer to nullptr after deletion
        }
    }

}
 
// Iterative function to insert a key into a Trie
void Trie::insert(std::string key, int wordIndex)
{
    // start from the root node
    Trie* curr = this;
    for (auto c : key)
    {
        // create a new node if the path doesn't exist
        if (curr->character[c - '0'] == nullptr) {
            curr->character[c - '0'] = new Trie();
        }
 
        // go to the next node
        curr = curr->character[c - '0'];
    }
 
    // mark the current node as a leaf
    curr->isLeaf = true;
    curr->wordOccurenceIndex.push_back(wordIndex);
    // std::cout<<"key inserted: "<<key<<", occurence index: "<<wordIndex<<std::endl;
}
 
// Iterative function to search a key in a Trie. It returns n'th occurence of the key
// if the key is found in the Trie; otherwise, it returns -1
std::string Trie::search(std::string key, int occurence)
{
    Trie* curr = this;
    for (auto c : key)
    {
        // go to the next node
        curr = curr->character[c - '0'];
 
        // if the string is invalid (reached end of a path in the Trie)
        if (curr == nullptr) {
            return "No matching entry";
        }
    }
 
    // return n'th occurence of key if the current node is a leaf and the
    // end of the string is reached
    if(occurence > static_cast<int>(curr->wordOccurenceIndex.size()))
        return "No matching entry";
    // std::cout<<"word occurence index: "<<curr->wordOccurenceIndex[occurence - 1]<<", size: "<<curr->wordOccurenceIndex.size()<<std::endl;


    return std::to_string(curr->wordOccurenceIndex[occurence - 1]);
}
}
}
