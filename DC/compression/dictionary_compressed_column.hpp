
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
};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), dc_vector(), dictionary(){
	}

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any& new_value){
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			 return this->insert(value);
		}
		
		
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const T& new_value){
		
		unsigned pos;
		pos = find(dictionary.begin(), dictionary.end(), new_value) - dictionary.begin();
		if(pos < dictionary.size())
		{
			dc_vector.push_back(pos);
		}
		else		
		{
			dictionary.push_back(new_value);
			dc_vector.push_back(dictionary.size() - 1);
		}	
		return true;
	}
	
	template <typename T> 
	template <typename InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator first, InputIterator last){
		
		for (InputIterator it=first; it != last; ++it)
		{
			if (!this->insert(*it))
			{
				return false;
			}
		}
		
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
			unsigned pos;
			pos = find(dictionary.begin(), dictionary.end(), value) - dictionary.begin();
			if(pos < dictionary.size())
			{
				dc_vector[tid] = pos;
			}
			else		
			{
				dictionary.push_back(value);
				dc_vector[tid] = (dictionary.size() - 1);
			}
			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){	
		if(!tids)
			return false;
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){		 
			T value = boost::any_cast<T>(new_value);
			unsigned size = dc_vector.size();
			vector<T> dictionary_ = dictionary;
			vector<int> dc_vector_ = dc_vector;
			clearContent();

			for(unsigned int id=0;id<tids->size();id++){
				TID tid=(*tids)[id];
				for(TID i = 0; i < size; i ++)	{
					if(i!=tid) {
						this->insert(dictionary_[dc_vector_[i]]);
					}
					else
						this->insert(value);
			 	}
		 	}
			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
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

		typename PositionList::reverse_iterator rit;

		//delete tuples in reverse order, otherwise the first deletion would invalidate all other tids
		for (rit = tids->rbegin(); rit!=tids->rend(); ++rit)
			dc_vector.erase(dc_vector.begin()+(*rit));
			
		return true;			
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
	
		dc_vector.clear();
		dictionary.clear();
		return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::store(const std::string& path_){
		std::string path(path_);
		path += "/";
		path += this->name_;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << dc_vector;

		outfile.flush();
		outfile.close();

		std::string path2(path_);
		path2 += "/dict";
		path2 += this->name_;
		std::ofstream outfile2 (path2.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa2(outfile2);

		oa2 << dictionary;

		outfile2.flush();
		outfile2.close();
		
		return true;
	}
	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path_){
	
		std::string path(path_);
		path += "/";
		path += this->name_;
		
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> dc_vector;
		infile.close();

		std::string path2(path_);
		path2 += "/dict";
		path2 += this->name_;
	
		std::ifstream infile2 (path2.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia2(infile2);
		ia2 >> dictionary;
		infile2.close();
		return true;
	}

	template<class T>
	T& DictionaryCompressedColumn<T>::operator[](const int index){
		return dictionary[dc_vector[index]];
	}

	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
		return dc_vector.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

