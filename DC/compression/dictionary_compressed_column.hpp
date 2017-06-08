
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>
using namespace std;

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DictionaryCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	template <typename InputIterator>
	bool insert(InputIterator first, InputIterator last);

	//T ikramCompression(const T& ikramVal);
	virtual bool update(TID tid, const boost::any& new_value);
	virtual bool update(PositionListPtr tid, const boost::any& new_value);	
	
	virtual bool remove(TID tid);
	//assumes tid list is sorted ascending
	virtual bool remove(PositionListPtr tid);
	virtual bool clearContent();

	virtual const boost::any get(TID tid);
	//virtual const boost::any* const getRawData()=0;
	virtual void print() const throw();
	virtual size_t size() const throw();
	virtual unsigned int getSizeinBytes() const throw();

	virtual const ColumnPtr copy() const;

	virtual bool store(const std::string& path);
	virtual bool load(const std::string& path);


	
	virtual T& operator[](const int index);
	
	/*! values*/
	std::vector<int> dc_vector;
	std::vector<T> dictionary;
	//std::vector<int> indexdc_vector;

};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), dc_vector(), dictionary(){

		std::cout << "Ikram : DCC created " << std::endl;
		
	}

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any& new_value){
		//cout << new_value << endl;
		
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			cout << value << endl;
			 dc_vector.push_back(1);
			 return true;
		}
		
		
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const T& new_value){
		
		int pos;
		//std::cout << "ikram : in insert value is -> " << new_value << endl;
		//print();
		pos = find(dictionary.begin(), dictionary.end(), new_value) - dictionary.begin();
		if(pos < dictionary.size())
		{
			cout << "already" << endl;
			dc_vector.push_back(pos);
		}
		else		
		{
			dictionary.push_back(new_value);
			dc_vector.push_back(dictionary.size() - 1);
		}
		
	
		//dc_vector.push_back(new_value);
		
		return true;
	}
	
	template <typename T> 
	template <typename InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator first, InputIterator last){
		
		this->dc_vector.insert(this->dc_vector.end(),first,last);
		
		return true;
	}

	template<class T>
	const boost::any DictionaryCompressedColumn<T>::get(TID tid){
		
		if(tid<dc_vector.size())
		{
			
 			return boost::any(dictionary[dc_vector[tid]]);
		}
		else{
			std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid  << std::endl;
		}
		return boost::any();
	}

	template<class T>
	void DictionaryCompressedColumn<T>::print() const throw(){
		
		std::cout << "| " << this->name_ << " |" << std::endl;
		std::cout << "________________________" << std::endl;
		for(unsigned int i=0;i<dc_vector.size();i++){
			std::cout << "| " << dc_vector[i] << " |" << std::endl;
		}
	}
	template<class T>
	size_t DictionaryCompressedColumn<T>::size() const throw(){

		return dc_vector.size();
	}
	template<class T>
	const ColumnPtr DictionaryCompressedColumn<T>::copy() const{

		return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(TID tid, const boost::any& new_value){
		
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			int pos;
			pos = find(dictionary.begin(), dictionary.end(), value) - dictionary.begin();
		if(pos < dictionary.size())
		{
			cout << "already" << endl;
			dc_vector[tid] = pos;
		}
		else		
		{
			dictionary.push_back(value);
			dc_vector[tid] = (dictionary.size() - 1);
		}
			 //dc_vector[tid]= 1;
			 return true;
		}else{
			std::cout << "Ikram : Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){
		//cout << new_value << endl;		
		
		if(!tids)
			return false;
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			 for(unsigned int i=0;i<tids->size();i++){
				TID tid=(*tids)[i];
				cout << value << endl << tid << endl;
			 }
			 return true;
		}else{
			std::cout << "Ikram : Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		
		return false;		
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(TID tid){
	
		dc_vector.erase(dc_vector.begin()+tid);
		
		return true;	
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr tids){
	
		if(!tids)
			return false;
		//test whether tid list has at least one element, if not, return with error
		if(tids->empty())
			return false;		

		//assert();

		typename PositionList::reverse_iterator rit;

		for (rit = tids->rbegin(); rit!=tids->rend(); ++rit)
			dc_vector.erase(dc_vector.begin()+(*rit));

		/*
		//delete tuples in reverse order, otherwise the first deletion would invalidate all other tids
		unsigned int i=tids->size()-1;
		while(true)	
			TID = (*tids)[i];
			dc_vector.erase(dc_vector.begin()+tid);		
			if(i==0) break;
		}*/
		
		
		return true;			
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
	
		dc_vector.clear();
		
		return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::store(const std::string& path_){
	
		//string path("data/");
		std::string path(path_);
		path += "/";
		path += this->name_;
		//std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << dc_vector;

		outfile.flush();
		outfile.close();
		
		return true;
	}
	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path_){
	
		std::string path(path_);
		//std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
		//string path("data/");
		path += "/";
		path += this->name_;
		
		//std::cout << "Opening File '" << path << "'..." << std::endl;
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> dc_vector;
		infile.close();


		return true;
	}

	template<class T>
	T& DictionaryCompressedColumn<T>::operator[](const int index){
		//static T t;
		//return t;
		
		return dictionary[dc_vector[index]];
	}

	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
		//return 0; 
		return dc_vector.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

