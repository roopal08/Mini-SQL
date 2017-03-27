#include <map>
#include <stdio.h>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <string.h>
#include <algorithm>
#include <sstream>
#include <iterator>
#include <iomanip>
#include <set>
#include <limits>
using namespace std;

map<string, vector<string> > db_schema;
map<string, vector<vector <int> > > db_data;
vector<string> tableName;
vector<string> tableSearch;
vector<string> columnName;
vector<vector <int> > jointTable;
vector <int> columnNum;
typedef map <string, vector<string> >::const_iterator MapIterator;
void read_schema()
{
	ifstream myfile ("metadata.txt");
	string line,dummy,table_name;
	vector<string> cols;
	int flag;
	if(myfile.is_open())
	{
		while(getline(myfile,line))
		{
			if(line[line.length()-1]==13)
			{
				dummy = "";
				dummy.append(line,0,line.length()-1);
			}
			else
			{
				dummy = "";
				dummy.append(line,0,line.length());
			}
			if(dummy.compare("<begin_table>") == 0)
			{
				flag = 1;
				continue;
			}
			else if(dummy.compare("<end_table>") == 0)
			{
				db_schema.insert(pair<string, vector<string> >(table_name,cols));
				cols.clear();
				flag = 0;
				continue;
			}
			else if(flag == 1)
			{
				table_name = "";
				table_name.append(dummy);
				flag = 2;
			}
			else if(flag == 2)
			{
				cols.push_back(dummy);
			}
		}
	}
	myfile.close();

}
void read_data()
{
	vector<int> myints;
	char file_name[100];
	string filename,line;
	vector<vector <int> > myrows;
	for(MapIterator it = db_schema.begin(); it != db_schema.end(); it++)
	{
		myrows.clear();
		filename = "";
		filename.append(it->first);
		filename.append(".csv");
		filename.copy(file_name,filename.length(),0);
		file_name[filename.length()] = '\0';
		ifstream datafile (file_name);
		if(datafile.is_open())
		{
			while(getline(datafile,line))
			{
				myints.clear();
				replace(line.begin(), line.end(), ',', ' ');
				line.erase(remove(line.begin(),line.end(),'\"'),line.end());
				stringstream stream(line);
				while(1)
				{
					int n;
					stream >> n;
					if(!stream)
					{
						break;
					}
					myints.push_back(n);
				}
				myrows.push_back(myints);

			}
		}

		db_data.insert(pair<string, vector <vector <int> > >(it->first,myrows));
		datafile.close();
	}

}
//Not handled if same column is present in two tables
int give_tablenames()
{
	int size = tableSearch.size();
	if(size>1)
	{
		MapIterator it1 = db_schema.find(tableName[0]);
		MapIterator it2 = db_schema.find(tableName[1]);
		//if((find(it1->second.begin(),it1->second.end(),tableSearch[0]) != it1->second.end()) && (find(it2->second.begin(),it2->second.end(),tableSearch[1])!= it2->second.end()))
		//	return 0;

		for(int i = 0; i < tableSearch.size(); i++)
		{
			if((find(it1->second.begin(),it1->second.end(),tableSearch[i]) != it1->second.end()) && (find(it2->second.begin(),it2->second.end(),tableSearch[i])!= it2->second.end()))
				return 0;
			for(int j = 0; j < tableName.size(); j++)
			{
				MapIterator it = db_schema.find(tableName[j]);
				if(find(it->second.begin(),it->second.end(),tableSearch[i]) != it->second.end())
				{
					string name = "";
					name.append(tableName[j]);
					name.append(".");
					name.append(tableSearch[i]);
					columnName.push_back(name);
					size--;
					break;
				}
			}

		}
	}
	else if(size == 1)
	{
		MapIterator it1 = db_schema.find(tableName[0]);
		for(int i = 0; i < tableSearch.size(); i++)
		{
			for(int j = 0; j < tableName.size(); j++)
			{
				MapIterator it = db_schema.find(tableName[j]);
				if(find(it->second.begin(),it->second.end(),tableSearch[i]) != it->second.end())
				{
					string name = "";
					name.append(tableName[j]);
					name.append(".");
					name.append(tableSearch[i]);
					columnName.push_back(name);
					size--;
					break;
				}
			}

		}

	}
	if(size == 0)
		return 1;
	else
		return 0;

}
int check_query(string token)
{

	if(token.compare("sum") == 0)
	{
		return 2;
	}
	else if(token.compare("avg") == 0)
	{
		return 3;
	}
	else if(token.compare("min") == 0)
	{
		return 4;
	}
	else if(token.compare("distinct") == 0)
	{
		return 5;
	}
	else if(token.compare("max") == 0)
	{
		return 1;
	}
	else
	{
		return 0; 
	}
}
void buildJoint()
{
	sort(tableName.begin(),tableName.end());
	//cout << "tanu\n";	
	//sort(columnName.begin(),columnName.end());
	if(tableName.size() == 1)
	{
		jointTable = db_data.find(tableName[0])->second;
	}
	else
	{
		int counter = 0;
		vector< vector <int> > table1 = db_data.find(tableName[0])->second;
		vector< vector <int> > table2 = db_data.find(tableName[1])->second;
		vector<int> comb;
		for(int  it1 = 0; it1 < table1.size(); it1++)
		{
			for(int it2 = 0; it2 < table2.size(); it2++)
			{
				comb.clear();
				comb.insert(comb.end(), table1[it1].begin(), table1[it1].end());
				comb.insert(comb.end(), table2[it2].begin(), table2[it2].end());
				jointTable.push_back(comb);
			}
		}
	}
}
void parse_num_query(int num_query)
{

	if(num_query == 1)
	{
		int a = numeric_limits<int>::min();
		for(int it1 = 0; it1 < jointTable.size(); it1++)
		{
			if(jointTable[it1][columnNum[0]] > a)
				a = jointTable[it1][columnNum[0]];

		}
		cout << a << "\n";
	}
	else if(num_query == 4)
	{
		int a = numeric_limits<int>::max();
		for(int it1 = 0; it1 < jointTable.size(); it1++)
		{
			if(jointTable[it1][columnNum[0]] < a)
				a = jointTable[it1][columnNum[0]];

		}
		cout << a << "\n";
	}
	else if(num_query == 2)
	{
		int a = 0;
		for(int it1 = 0; it1 < jointTable.size(); it1++)
		{
			a+= jointTable[it1][columnNum[0]];

		}
		cout << a << "\n";
	}
	else if(num_query == 3)
	{
		double a = 0;
		for(int it1 = 0; it1 < jointTable.size(); it1++)
		{
			a+= jointTable[it1][columnNum[0]];
		}
		a/=jointTable.size();
		cout << a << "\n";
	}
}
bool is_number(const string& s){
	for(int i = 0; i < s.length(); i++)//for each char in string,
		if(! (s[i] >= '0' && s[i] <= '9' || s[i] == ' ') ) return false;
	//if s[i] is between '0' and '9' of if it's a whitespace (there may be some before and after
	// the number) it's ok. otherwise it isn't a number.

	return true;
}
int main(int argc, char* argv[])
{
	if(argc!=2)
	{
		printf("No query given\n");
		return 0;
	}
	else
	{
		read_schema();
		read_data();
		string query(argv[1]);
		string distinct;
		int num_query=0,jointype=0,condition = 0,cond1 = 0,cond2 = 0;
		string join1, join2,join3,join4;
		int flag = -1,full_col = 0, iswhere = 0;
		replace(query.begin(), query.end(), ',', ' ');
		istringstream iss(query);
		string word;
		int counter = 0;
		while(iss >> word)
		{
			if(flag == -1)
			{
				transform(word.begin(), word.end(), word.begin(), ::tolower);
				if(word.compare("select") != 0)
				{
					cout << "99Wrong query\n";
					return 0;
				}
				flag = 0;
			}
			else if(flag == 0)
			{
				if(word.compare("from") == 0 || word.compare("From") == 0 || word.compare("FROM") == 0)
				{
					flag = 1;
					//cout << "tt";
					continue;
				}
				else
				{
					if(word.compare("*") == 0)
					{
						full_col = 1;
						continue;
					}
					else if(word.find('(')!=string::npos)
					{
						string temp = "";
						temp.append(word);
						replace(word.begin(), word.end(),'(', ' ');
						replace(word.begin(),word.end(),')',' ');
						istringstream db(word);
						vector<string> tokens;
						copy(istream_iterator<string>(db),
								istream_iterator<string>(),
								back_inserter(tokens));
						num_query = check_query(tokens[0]);
						if(num_query == 0)
						{
							cout << "88wrong query" << "\n";
							return 0;
						}
						word = "";
						word.append(tokens[1]);
						if(num_query > 0 && num_query < 5)
						{
							cout << temp << "\n";
						}
						else if(num_query == 5)
						{
							distinct = tokens[1];
						}
					}
					if(word.find(".")!=string::npos)
					{
						string temp = "";
						temp.append(word);
						replace(word.begin(), word.end(), '.', ' ');
						istringstream db(word);
						vector<string> tokens;
						copy(istream_iterator<string>(db),
								istream_iterator<string>(),
								back_inserter(tokens));
						if(db_schema.find(tokens[0])== db_schema.end())
						{
							cout << "3wrong query" << "\n";
							return 0;
						}
						else
						{
							MapIterator it = db_schema.find(tokens[0]);
							if(find(it->second.begin(),it->second.end(),tokens[1])==it->second.end())
							{
								cout << "2wrong query" << "\n";
								return 0;
							}
						}
						columnName.push_back(temp);
						//cout << "guu\n";
					}
					else
					{
						//cout << "mummy" << "\n";
						tableSearch.push_back(word);
					}
				}
			}
			else if(flag == 1)
			{
				if(word.compare("where") == 0 || word.compare("Where") == 0 || word.compare("WHERE") == 0)
				{
					iswhere = 1;
					flag = 2;
					continue;
				}
				else
				{
					replace(word.begin(), word.end(), ',', ' ');
					if(db_schema.find(word)== db_schema.end())
					{
						cout << "5wrong query" << "\n";
						return 0;
					}
					else
					{
						tableName.push_back(word);
					}


				}
			}
			else if(flag == 2)
			{
				//Cannot hanle <= and >=
				if(word.find("=")!=string::npos)
				{
					int found = word.find("=");
					join1 = "";
					join2 = "";
					join1.append(word,0, found);
					join2.append(word,found + 1, word.size() - found - 1);
					if(!is_number(join2))
					{	
						string t1 = "";
						t1.append(join1, 0, join1.find("."));
						if(t1.compare(tableName[0])!=0)
						{
							string temp = join1;
							join1 = join2;
							join2 = temp;
						}
						string temp = join1;
						join1 = "";
						join1.append(temp, temp.find(".")+1, temp.size() - 1 - temp.find("."));
						temp = join2;
						join2 = "";
						join2.append(temp, temp.find(".")+1, temp.size() - 1 - temp.find("."));
					}
					else
					{
						jointype = 3;
						condition = 1;
						string t1 = "";
						t1.append(join1, 0, join1.find("."));
						if(t1.compare(tableName[0])!=0 && t1.compare(tableName[1])!=0)
						{
							cout << "5wrong query\n";
							return 0;
						}
					}
				}
				else if(word.find(">")!=string::npos || word.find("<")!=string::npos)
				{
					int found;
					if(word.find(">")!=string::npos)
					{
						found = word.find(">");
						condition = 3;
					}
					else
					{
						found = word.find("<");
						condition = 2;
					}
					join1 = "";
					join2 = "";
					join1.append(word,0, found);
					join2.append(word,found + 1, word.size() - found - 1);
					if(!is_number(join2))
					{
						cout << "1wrong query\n";
						return 0;
					}
					else
					{
						string t1 = "";
						t1.append(join1, 0, join1.find("."));
						if(t1.compare(tableName[0])!=0 && t1.compare(tableName[1])!=0)
						{
							cout << "2wrong query\n";
							return 0;
						}

					}
					jointype = 3;
				}
				flag = 3;
			}
			else if(flag == 3)
			{
				if((word.compare("AND") == 0) || (word.compare("and")==0))
					jointype = 1;
				else if((word.compare("OR") == 0) || (word.compare("or")==0))
					jointype = 2;
				flag = 4;
			}
			else if(flag == 4)
			{
				int found;
				if(word.find("=")!=string::npos)
				{
					found = word.find("=");
					cond2 = 1;
				}
				else if(word.find("<")!=string::npos)
				{
					found = word.find("<");
					cond2 = 2;
				}
				else if(word.find(">")!=string::npos)
				{
					found = word.find("<");
					cond2 = 2;
				}
				cond1 = condition;
				join3 = "";
				join4 = "";
				join3.append(word,0, found);
				join4.append(word,found + 1, word.size() - found - 1);
				if(!is_number(join4))
				{
					cout << "query not supported\n";
				}
			}

		}
		if(tableSearch.size() > 0)
		{
			if(!give_tablenames())
			{
				cout << "53wrong query\n";
				return 0;
			}
			tableSearch.clear();

		}
		buildJoint();
		const char separator    = ' ';
		const int nameWidth     = 15;
		const int numWidth      = 8;
		if(iswhere == 0 && full_col == 1)
		{

			for(int it1 = 0; it1 < tableName.size(); it1++)
			{
				MapIterator it = db_schema.find(tableName[it1]);
				for(int it2 = 0; it2 < it->second.size(); it2++)
				{
					string temp = "";
					temp.append(tableName[it1]);
					temp.append(".");
					temp.append(it->second[it2]);
					cout << left << setw(nameWidth) << setfill(separator) << temp;

				}
			}
			cout << endl;

			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < jointTable[it1].size(); it2++)
				{
					cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
				}
				cout << endl;
			}
		}
		else if(iswhere == 0 && full_col == 0)
		{
			columnNum.clear();
			int found;
			string T_name,C_name;
			int size = db_schema.find(tableName[0])->second.size();
			for(int it1 = 0; it1 < columnName.size(); it1++)
			{
				found = columnName[it1].find(".");
				T_name = "";
				T_name.append(columnName[it1], 0, found);
				MapIterator it = db_schema.find(T_name);
				C_name = "";
				C_name.append(columnName[it1], found+1, columnName[it1].size() - found - 1);
				for(int it2 = 0 ; it2 < it->second.size(); it2++)
				{
					if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[0]) == 0)
					{
						columnNum.push_back(it2);
						//cout << it2 << "\n";
					}
					else if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[1]) == 0)
					{
						columnNum.push_back(it2 + size);
						//cout << it2 + size<< "\n";
					}
				}
			}
			if(num_query == 0)
			{
				for(int it1 = 0; it1 < columnName.size(); it1++)
					cout << left << setw(nameWidth) << setfill(separator) << columnName[it1];
				cout << endl;
				for(int it1 = 0; it1 < jointTable.size(); it1++)
				{
					for(int it2 = 0; it2 < columnNum.size(); it2++)
					{
						cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][columnNum[it2]];

					}
					cout << endl;
				}
			}
			else if(num_query != 5)
			{
				parse_num_query(num_query);
				return 0;
			}
			else if(num_query == 5)
			{
				//cout << "hello";
				int dis_col;
				MapIterator it = db_schema.find(tableName[0]);
				for(int it1 = 0 ; it1 < it->second.size(); it1++)
				{

					//cout << left << setw(nameWidth) << setfill(separator) << it->second[it1];
					//cout << "hello" << "\n";
					if(it->second[it1].compare(distinct) == 0)
					{
						dis_col = it1;
						break;
					}
				}
				for(int it1 = 0; it1 < columnName.size(); it1++)
					cout << left << setw(nameWidth) << setfill(separator) << columnName[it1];
				cout << endl;
				//cout << dis_col << "\n";
				set <int> distinct_col;
				for(int it1 = 0; it1 < jointTable.size(); it1++)
				{
					if(distinct_col.find(jointTable[it1][dis_col]) == distinct_col.end())
					{
						distinct_col.insert(jointTable[it1][dis_col]);
						for(int it2 = 0; it2 < columnNum.size(); it2++)
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][columnNum[it2]];
							if(it2 == columnNum.size() - 1)
								cout << endl;
						}
					}
				}
			}
		}
		else if(iswhere == 1 && full_col == 1 && jointype == 0)
		{
			columnNum.clear();
			MapIterator it = db_schema.find(tableName[0]);
			for(int it1 = 0; it1 < it->second.size(); it1++)
			{
				if(it->second[it1].compare(join1) == 0)
				{
					columnNum.push_back(it1);
					break;
				}
			}
			int it2 = it->second.size();
			it = db_schema.find(tableName[1]);
			for(int it1 = 0; it1 < it->second.size(); it1++)
			{
				if(it->second[it1].compare(join2) == 0)
				{
					columnNum.push_back(it1+it2);
					break;
				}
			}
			for(int it1 = 0; it1 < tableName.size(); it1++)
			{
				MapIterator it = db_schema.find(tableName[it1]);
				for(int it2 = 0; it2 < it->second.size(); it2++)
				{
					string temp = "";
					temp.append(tableName[it1]);
					temp.append(".");
					temp.append(it->second[it2]);
					cout << left << setw(nameWidth) << setfill(separator) << temp;

				}

			}
			cout << endl;
			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < jointTable[it1].size(); it2++)
				{
					if(jointTable[it1][columnNum[0]] == jointTable[it1][columnNum[1]])
					{
						cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
						if(it2 == jointTable[it1].size()-1)
							cout << endl;
					}
				}
			}
		}
		else if(iswhere == 1 && full_col == 0 && jointype == 0)
		{
			vector<int> joinNum;
			joinNum.clear();
			MapIterator it = db_schema.find(tableName[0]);
			for(int it1 = 0; it1 < it->second.size(); it1++)
			{
				if(it->second[it1].compare(join1) == 0)
				{
					joinNum.push_back(it1);
					break;
				}
			}
			int it2 = it->second.size();
			it = db_schema.find(tableName[1]);
			for(int it1 = 0; it1 < it->second.size(); it1++)
			{
				if(it->second[it1].compare(join2) == 0)
				{
					joinNum.push_back(it1+it2);
					break;
				}
			}
			columnNum.clear();
			int found;
			string T_name,C_name;
			int size = db_schema.find(tableName[0])->second.size();
			for(int it1 = 0; it1 < columnName.size(); it1++)
			{
				found = columnName[it1].find(".");
				T_name = "";
				T_name.append(columnName[it1], 0, found);
				MapIterator it = db_schema.find(T_name);
				C_name = "";
				C_name.append(columnName[it1], found+1, columnName[it1].size() - found - 1);
				for(int it2 = 0 ; it2 < it->second.size(); it2++)
				{
					if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[0]) == 0)
					{
						columnNum.push_back(it2);
					}
					else if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[1]) == 0)
					{
						columnNum.push_back(it2 + size);
					}
				}
				cout << left << setw(nameWidth) << setfill(separator) << columnName[it1];
			}
			cout << endl;
			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < columnNum.size(); it2++)
				{
					if(jointTable[it1][joinNum[0]] == jointTable[it1][joinNum[1]])
					{
						cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][columnNum[it2]];	
						if(it2 == columnNum.size() - 1)
							cout << endl;
					}
					//cout << columnNum[it2] << "\n";
				}
			}
		}
		else if(iswhere == 1 && full_col == 1 && jointype==3)
		{
			//cout << "hello\n";
			columnNum.clear();
			MapIterator it = db_schema.find(tableName[0]);
			int found = join1.find(".");
			string t1 ="", t2 = "";
			t1.append(join1,0, found);
			t2.append(join1,found + 1, join1.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(size+it1);
				}
			}
			for(int it1 = 0; it1 < tableName.size(); it1++)
			{
				MapIterator it = db_schema.find(tableName[it1]);
				for(int it2 = 0; it2 < it->second.size(); it2++)
				{
					string temp = "";
					temp.append(tableName[it1]);
					temp.append(".");
					temp.append(it->second[it2]);
					cout << left << setw(nameWidth) << setfill(separator) << temp;

				}

			}
			cout << endl;
			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < jointTable[it1].size(); it2++)
				{
					if(condition == 1)
					{
						//cout << columnNum[0];
						if(jointTable[it1][columnNum[0]] == atoi(join2.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					else if(condition == 2)
					{
						if(jointTable[it1][columnNum[0]] < atoi(join2.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}
					}
					else if(condition == 3)
					{
						if(jointTable[it1][columnNum[0]] > atoi(join2.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}
					}
				}
			}
		}
		else if(iswhere == 1 && full_col == 0 && jointype == 3)
		{
			vector<int> joinNum;
			joinNum.clear();
			MapIterator it = db_schema.find(tableName[0]);
			int found = join1.find(".");
			string t1 ="", t2 = "";
			t1.append(join1,0, found);
			t2.append(join1,found + 1, join1.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						joinNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						joinNum.push_back(size+it1);
				}
			}
			columnNum.clear();
			string T_name,C_name;
			int size = db_schema.find(tableName[0])->second.size();
			for(int it1 = 0; it1 < columnName.size(); it1++)
			{
				found = columnName[it1].find(".");
				T_name = "";
				T_name.append(columnName[it1], 0, found);
				MapIterator it = db_schema.find(T_name);
				C_name = "";
				C_name.append(columnName[it1], found+1, columnName[it1].size() - found - 1);
				for(int it2 = 0 ; it2 < it->second.size(); it2++)
				{
					if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[0]) == 0)
					{
						columnNum.push_back(it2);
					}
					else if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[1]) == 0)
					{
						columnNum.push_back(it2 + size);
					}
				}
				cout << left << setw(nameWidth) << setfill(separator) << columnName[it1];
			}
			cout << endl;
			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < columnNum.size(); it2++)
				{
					if(condition == 1)
					{
						if(jointTable[it1][joinNum[0]] == atoi(join2.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][columnNum[it2]];	
							if(it2 == columnNum.size() - 1)
								cout << endl;
						}
					}
					else if(condition == 2)
					{
						if(jointTable[it1][joinNum[0]] < atoi(join2.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][columnNum[it2]];	
							if(it2 == columnNum.size() - 1)
								cout << endl;
						}
					}
					else if(condition == 3)
					{
						if(jointTable[it1][joinNum[0]] > atoi(join2.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][columnNum[it2]];	
							if(it2 == columnNum.size() - 1)
								cout << endl;
						}
					}

				}
			}
		}
		else if(iswhere == 1 && full_col == 1 && jointype==3)
		{
			cout << "hello\n";
			columnNum.clear();
			MapIterator it = db_schema.find(tableName[0]);
			int found = join1.find(".");
			string t1 ="", t2 = "";
			t1.append(join1,0, found);
			t2.append(join1,found + 1, join1.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(size+it1);
				}
			}
			for(int it1 = 0; it1 < tableName.size(); it1++)
			{
				MapIterator it = db_schema.find(tableName[it1]);
				for(int it2 = 0; it2 < it->second.size(); it2++)
				{
					string temp = "";
					temp.append(tableName[it1]);
					temp.append(".");
					temp.append(it->second[it2]);

				}
			}
		}
		else if(iswhere == 1 && full_col == 1 && jointype==1)
		{
			//cout << "hello\n";
			columnNum.clear();
			int found = join1.find(".");
			string t1 ="", t2 = "";
			t1.append(join1,0, found);
			t2.append(join1,found + 1, join1.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(size+it1);
				}
			}
			found = join3.find(".");
			t1 ="";
			t2 = "";
			t1.append(join3,0, found);
			t2.append(join3,found + 1, join3.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						columnNum.push_back(size+it1);
				}
			}
			vector<vector <int> > jointTable1;
			vector<int> comb;
			for(int it1 = 0; it1 < tableName.size(); it1++)
			{
				MapIterator it = db_schema.find(tableName[it1]);
				for(int it2 = 0; it2 < it->second.size(); it2++)
				{
					string temp = "";
					temp.append(tableName[it1]);
					temp.append(".");
					temp.append(it->second[it2]);
					cout << left << setw(nameWidth) << setfill(separator) << temp;

				}

			}
			cout << endl;
			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < jointTable[it1].size(); it2++)
				{
					if(cond1 == 1 && cond2 == 1)
					{
						if(jointTable[it1][columnNum[0]] == atoi(join2.c_str()) && jointTable[it1][columnNum[1]] == atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 1 && cond2 == 2)
					{
						if(jointTable[it1][columnNum[0]] == atoi(join2.c_str()) && jointTable[it1][columnNum[1]] < atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 1 && cond2 == 3)
					{
						if(jointTable[it1][columnNum[0]] == atoi(join2.c_str()) && jointTable[it1][columnNum[1]] > atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 2 && cond2 == 1)
					{
						if(jointTable[it1][columnNum[0]] < atoi(join2.c_str()) && jointTable[it1][columnNum[1]] == atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 2 && cond2 == 2)
					{
						if(jointTable[it1][columnNum[0]] < atoi(join2.c_str()) && jointTable[it1][columnNum[1]] < atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 2 && cond2 == 3)
					{
						if(jointTable[it1][columnNum[0]] < atoi(join2.c_str()) && jointTable[it1][columnNum[1]] > atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 3 && cond2 == 1)
					{
						if(jointTable[it1][columnNum[0]] > atoi(join2.c_str()) && jointTable[it1][columnNum[1]] == atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 3 && cond2 == 2)
					{
						if(jointTable[it1][columnNum[0]] > atoi(join2.c_str()) && jointTable[it1][columnNum[1]] < atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
					if(cond1 == 3 && cond2 == 3)
					{
						if(jointTable[it1][columnNum[0]] > atoi(join2.c_str()) && jointTable[it1][columnNum[1]] > atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == jointTable[it1].size()-1)
								cout << endl;
						}

					}
				}
			}

		}
		else if(full_col == 0 && jointype == 1 && iswhere == 1)
		{
			//cout << "hello\n";
			vector<int> joinNum;
			joinNum.clear();
			int found = join1.find(".");
			string t1 ="", t2 = "";
			t1.append(join1,0, found);
			t2.append(join1,found + 1, join1.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						joinNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						joinNum.push_back(size+it1);
				}
			}
			found = join3.find(".");
			t1 ="";
			t2 = "";
			t1.append(join3,0, found);
			t2.append(join3,found + 1, join3.size() - found - 1);
			//cout << t1 << t2 << "\n";
			if(t1.compare(tableName[0]) == 0)
			{
				//cout << "rashi";
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						joinNum.push_back(it1);
				}
			}
			else if(t1.compare(tableName[1])==0)
			{
				int size = db_schema.find(tableName[0])->second.size(); 
				MapIterator it = db_schema.find(t1);
				for(int it1 = 0; it1 < it->second.size(); it1++)
				{
					if(it->second[it1].compare(t2) == 0)
						joinNum.push_back(size+it1);
				}
			}
			columnNum.clear();
			string T_name,C_name;
			int size = db_schema.find(tableName[0])->second.size();
			for(int it1 = 0; it1 < columnName.size(); it1++)
			{
				found = columnName[it1].find(".");
				T_name = "";
				T_name.append(columnName[it1], 0, found);
				MapIterator it = db_schema.find(T_name);
				C_name = "";
				C_name.append(columnName[it1], found+1, columnName[it1].size() - found - 1);
				for(int it2 = 0 ; it2 < it->second.size(); it2++)
				{
					if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[0]) == 0)
					{
						columnNum.push_back(it2);
					}
					else if(it->second[it2].compare(C_name) == 0 && T_name.compare(tableName[1]) == 0)
					{
						columnNum.push_back(it2 + size);
					}
				}
				cout << left << setw(nameWidth) << setfill(separator) << columnName[it1];
			}
			cout << endl;
			for(int it1 = 0; it1 < jointTable.size(); it1++)
			{
				for(int it2 = 0; it2 < columnNum.size(); it2++)
				{
					if(cond1 == 1 && cond2 == 1)
					{
						if(jointTable[it1][joinNum[0]] == atoi(join2.c_str()) && jointTable[it1][joinNum[1]] == atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 1 && cond2 == 2)
					{
						if(jointTable[it1][joinNum[0]] == atoi(join2.c_str()) && jointTable[it1][joinNum[1]] < atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 1 && cond2 == 3)
					{
						if(jointTable[it1][joinNum[0]] == atoi(join2.c_str()) && jointTable[it1][joinNum[1]] > atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 2 && cond2 == 1)
					{
						if(jointTable[it1][joinNum[0]] < atoi(join2.c_str()) && jointTable[it1][joinNum[1]] == atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 2 && cond2 == 2)
					{
						if(jointTable[it1][joinNum[0]] < atoi(join2.c_str()) && jointTable[it1][joinNum[1]] < atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 2 && cond2 == 3)
					{
						if(jointTable[it1][joinNum[0]] < atoi(join2.c_str()) && jointTable[it1][joinNum[1]] > atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 3 && cond2 == 1)
					{
						if(jointTable[it1][joinNum[0]] > atoi(join2.c_str()) && jointTable[it1][joinNum[1]] == atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 3 && cond2 == 2)
					{
						if(jointTable[it1][joinNum[0]] > atoi(join2.c_str()) && jointTable[it1][joinNum[1]] < atoi(join4.c_str()))
						{

							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
					if(cond1 == 3 && cond2 == 3)
					{
						if(jointTable[it1][joinNum[0]] > atoi(join2.c_str()) && jointTable[it1][joinNum[1]] > atoi(join4.c_str()))
						{
							cout << left << setw(nameWidth) << setfill(separator) << jointTable[it1][it2];
							if(it2 == columnNum.size()-1)
								cout << endl;
						}

					}
				}
			}

		}
	}
return 0;
}
