
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */
template<class T>
class DECompressedColumn : public CompressedColumn<T>{
public:


	std::vector<T> compressed_vector;
	std::vector<int> icompressed_vector;
	int iTotalValue;
	T TotalValue;
	T Return_Variable;


	/***************** constructors and destructor *****************/
	DECompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DECompressedColumn();

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

};


/***************** Start of Implementation Section ******************/


	template<class T>
	DECompressedColumn<T>::DECompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type),compressed_vector(),icompressed_vector(),iTotalValue(0),TotalValue(0),Return_Variable(0) {

	}

	template<>
	DECompressedColumn<std::string>::DECompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<std::string>(name, db_type),compressed_vector(),icompressed_vector(),iTotalValue(0),TotalValue(""),Return_Variable("") {

	}

	template<class T>
	DECompressedColumn<T>::~DECompressedColumn(){

	}

	template<class T>
	bool DECompressedColumn<T>::insert(const boost::any& new_Value)
	{

		if(new_Value.empty()) return false;
		if(typeid(T)==new_Value.type()){
			T value = boost::any_cast<T>(new_Value);
			return this->insert(value);
		}
		return false;
	}

	template<class T>
	bool DECompressedColumn<T>::insert(const T& value)
	{

		T insertvalue;
		if(compressed_vector.size()>0)
		{
			insertvalue=value-TotalValue;
			TotalValue+=insertvalue;

		}
		else
		{
			insertvalue=value;
			TotalValue=insertvalue;
		}

		compressed_vector.push_back(insertvalue);

		return true;
	}

	template<>
	bool DECompressedColumn<float>::insert(const float& value)
	{
		int insertvalue;

		if(icompressed_vector.size()>0)
		{
			insertvalue=reinterpret_cast<const int*>(&value)[0]-iTotalValue;
			iTotalValue+=insertvalue;

		}
		else
		{
			insertvalue=reinterpret_cast<const int*>(&value)[0];
			iTotalValue=insertvalue;
		}
		icompressed_vector.push_back(insertvalue);

		return true;
	}

	std::string stringsub(std::string a,std::string b)
	{
		std::string diff = "";
		unsigned i=0;
		while(i<a.length() || i<b.length())
		{
			char ai,bi;
			if(i>=a.length())
				ai=0;
			else
				ai=a[i];
			if(i>=b.length())
				bi=0;
			else
				bi=b[i];
		
			diff.push_back(ai-bi);		
			i++;
		}
		return diff;
	}

	std::string stringadd(std::string a,std::string b)
	{
		std::string diff = "";
		unsigned i=0;
		while(i<a.length() || i<b.length())
		{
			char ai,bi;
			if(i>=a.length())
				ai=0;
			else
				ai=a[i];
			if(i>=b.length())
				bi=0;
			else
				bi=b[i];
		
			diff.push_back(ai+bi);		
			i++;
		}
		return diff;
	}

	template<>
	bool DECompressedColumn<std::string>::insert(const std::string& value)
	{
		std::string insertvalue;
		if(compressed_vector.size()>0)
		{
			insertvalue=stringsub(value,TotalValue);
			TotalValue=stringadd(TotalValue,insertvalue);

		}
		else
		{
			insertvalue=value;
			TotalValue=insertvalue;
		}

		compressed_vector.push_back(insertvalue);

		return true;
	}


	template <typename T>
	template <typename InputIterator>
	bool DECompressedColumn<T>::insert(InputIterator start , InputIterator end )
	{
		for (InputIterator it=start; it != end; ++it)
		{
			if (!this->insert(*it))
			{
				return false;
			}
		}

		return true;
	}


	template<class T>
	const boost::any DECompressedColumn<T>::get(TID index)
	{
		T value=0;
		for(TID i=0;i<=index;i++)
		{
			value+=compressed_vector[i];
		}

		return value;
	}

	template<>
	const boost::any DECompressedColumn<float>::get(TID index)
	{
		int value=0;

		for(TID i=0;i<=index;i++)
		{
			value+=icompressed_vector[i];
		}

		return reinterpret_cast<float*>(&value)[0];
	}


	template<>
	const boost::any DECompressedColumn<std::string>::get(TID index)
	{
		std::string value="";


		for(TID i=0;i<=index;i++)
		{
			value=stringadd(value,compressed_vector[i]);
		}

		return value;
	}


	template<class T>
	void DECompressedColumn<T>::print() const throw()
	{
		std::cout<<"The Delta encoded data is:"<<std::endl;
		T Total=0;
		for(unsigned i=0; i<compressed_vector.size();i++)
		{
			Total+=compressed_vector[i];
			std::cout<<Total<<std::endl;
		}

	}

	template<>
	void DECompressedColumn<float>::print() const throw()
	{
		std::cout<<"The Delta encoded data is:"<<std::endl;
		int Total=0;
		for(unsigned i=0; i<icompressed_vector.size();i++)
		{
			Total+=icompressed_vector[i];
			std::cout<<reinterpret_cast<float*>(&Total)[0]<<std::endl;
		}

	}

	template<>
	void DECompressedColumn<std::string>::print() const throw()
	{
		std::cout<<"The Delta encoded data is:"<<std::endl;
		std::string Total="";
		for(unsigned i=0; i<compressed_vector.size();i++)
		{
			Total=stringadd(Total,compressed_vector[i]);
			std::cout<<Total<<std::endl;
		}
	}

	template<class T>
	size_t DECompressedColumn<T>::size() const throw()
	{

		return compressed_vector.size();
	}

	template<>
	size_t DECompressedColumn<float>::size() const throw()
	{

		return icompressed_vector.size();
	}


	template<class T>
	const ColumnPtr DECompressedColumn<T>::copy() const{

		return ColumnPtr(new DECompressedColumn<T>((*this)));
	}

	template<class T>
	bool DECompressedColumn<T>::update(TID id , const boost::any& updatesvalue )
	{
		if (id >= this->size()) return false;
		if(typeid(T)==updatesvalue.type()){
			T value = boost::any_cast<T>(updatesvalue);
	
			T updatedLastCount=0;
			T diff=0;
			updatedLastCount=compressed_vector[0];

			for(TID i=1;i<id;i++)
			{
				updatedLastCount+=compressed_vector[i];
			}
			diff=compressed_vector[id];
			compressed_vector[id]=value-updatedLastCount;
			diff-=compressed_vector[id];
			compressed_vector[id+1]+=diff;
			updatedLastCount+= compressed_vector[id];
			for(TID i=id+2;i<compressed_vector.size();i++)
			{
				updatedLastCount+=compressed_vector[i];
			}

			TotalValue=updatedLastCount;
			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		return false;
	}

	template<>
	bool DECompressedColumn<float>::update(TID id , const boost::any& updatesvalue )
	{
		if (id >= this->size()) return false;
		if(typeid(float)==updatesvalue.type()){
			float val = boost::any_cast<float>(updatesvalue);
			int value=reinterpret_cast<int*>(&val)[0];
			int updatedLastCount=0;
			int diff=0;

			for(TID i=0;i<id;i++)
			{
				updatedLastCount+=icompressed_vector[i];
			}
			diff=icompressed_vector[id];
			icompressed_vector[id]=value-updatedLastCount;
			diff-=icompressed_vector[id];
			icompressed_vector[id+1]+=diff;
			updatedLastCount+= icompressed_vector[id];
			for(TID i=id+2;i<icompressed_vector.size();i++)
			{
				updatedLastCount+=icompressed_vector[i];
			}

			iTotalValue=updatedLastCount;

			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		return false;

	}

	template<>
	bool DECompressedColumn<std::string>::update(TID id , const boost::any& updatesvalue )
	{
		if (id >= this->size()) return false;
		if(typeid(std::string)==updatesvalue.type()){
			std::string value = boost::any_cast<std::string>(updatesvalue);
			std::string updatedLastCount="";
			std::string diff="";

			for(TID i=0;i<id;i++)
			{
				updatedLastCount=stringadd(updatedLastCount,compressed_vector[i]);
			}
			diff=compressed_vector[id];
			compressed_vector[id]=stringsub(value,updatedLastCount);
			diff=stringsub(diff,compressed_vector[id]);
			compressed_vector[id+1]=stringadd(compressed_vector[id+1],diff);
			updatedLastCount= stringadd(updatedLastCount,compressed_vector[id]);
			for(TID i=id+2;i<compressed_vector.size();i++)
			{
				updatedLastCount=stringadd(updatedLastCount,compressed_vector[i]);
			}

			TotalValue=updatedLastCount;

			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		return false;

	}

	template<class T>
	bool DECompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){
		if(!tids)
			return false;
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			T value = boost::any_cast<T>(new_value);
			unsigned size = compressed_vector.size();
			std::vector<T> compressed_vector_ = compressed_vector;
			clearContent();

			for(unsigned int id=0;id<tids->size();id++){
				TID tid=(*tids)[id];
				for(TID i = 0; i < size; i ++)	{
				
					T pre_value=0;
					for(TID j=0;j<=i;j++)
					{
						pre_value+=compressed_vector_[j];
					}
				
					if(i!=tid) {
						this->insert(pre_value);
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

	template<>
	bool DECompressedColumn<float>::update(PositionListPtr tids, const boost::any& new_value){
		if(!tids)
			return false;
		if(new_value.empty()) return false;
		if(typeid(float)==new_value.type()){
			float value = boost::any_cast<float>(new_value);
			unsigned size = icompressed_vector.size();
			std::vector<int> icompressed_vector_ = icompressed_vector;
			icompressed_vector.clear();
			iTotalValue=0;

			for(unsigned int id=0;id<tids->size();id++){
				TID tid=(*tids)[id];
				for(TID i = 0; i < size; i ++)	{
				
					int pre_value=0;
					for(TID j=0;j<=i;j++)
					{
						pre_value+=icompressed_vector_[j];
					}
				
					if(i!=tid) {
						this->insert(reinterpret_cast<float*>(&pre_value)[0]);
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

	template<>
	bool DECompressedColumn<std::string>::update(PositionListPtr tids, const boost::any& new_value){
		if(!tids)
			return false;
		if(new_value.empty()) return false;
		if(typeid(std::string)==new_value.type()){
			std::string value = boost::any_cast<std::string>(new_value);
			unsigned size = compressed_vector.size();
			std::vector<std::string> compressed_vector_ = compressed_vector;
			compressed_vector.clear();
			TotalValue="";

			for(unsigned int id=0;id<tids->size();id++){
				TID tid=(*tids)[id];
				for(TID i = 0; i < size; i ++)	{
				
					std::string pre_value="";
					for(TID j=0;j<=i;j++)
					{
						pre_value=stringadd(pre_value,compressed_vector_[j]);
					}
				
					if(i!=tid) {
						this->insert(pre_value);
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
	bool DECompressedColumn<T>::remove(TID id)
	{
		T valueToDelete=compressed_vector[id];
		compressed_vector[id+1]+=valueToDelete;
		compressed_vector.erase(compressed_vector.begin()+id);
		return false;
	}

	template<>
	bool DECompressedColumn<float>::remove(TID id)
	{
		int valueToDelete=icompressed_vector[id];
		icompressed_vector[id+1]+=valueToDelete;
		icompressed_vector.erase(icompressed_vector.begin()+id);
		return false;
	}

	template<>
	bool DECompressedColumn<std::string>::remove(TID id)
	{
		std::string valueToDelete=compressed_vector[id];
		compressed_vector[id+1]=stringadd(compressed_vector[id+1],valueToDelete);
		compressed_vector.erase(compressed_vector.begin()+id);
		return false;
	}

	template<class T>
	bool DECompressedColumn<T>::remove(PositionListPtr tids){
		if(!tids)
			return false;
		//test whether tid list has at least one element, if not, return with error
		if(tids->empty())
			return false;		

		typename PositionList::reverse_iterator rit;

		//delete tuples in reverse order, otherwise the first deletion would invalidate all other tids
		for (rit = tids->rbegin(); rit!=tids->rend(); ++rit)
			this->remove((*rit));

		return true;			
	}

	template<class T>
	bool DECompressedColumn<T>::clearContent(){
		compressed_vector.clear();
		TotalValue=0;
		return true;
	}

	template<>
	bool DECompressedColumn<float>::clearContent(){
		icompressed_vector.clear();
		iTotalValue=0;
		return true;
	}

	template<>
	bool DECompressedColumn<std::string>::clearContent(){
		compressed_vector.clear();
		TotalValue="";
		return true;
	}


	template<class T>
	bool DECompressedColumn<T>::store(const std::string& path_)
	{
		std::string path(path_);
		path += "/";
		path += this->name_;
	
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << TotalValue;
		oa << compressed_vector;
		outfile.flush();
		outfile.close();

		return true;
	}
	template<>
	bool DECompressedColumn<float>::store(const std::string& path_)
	{
		std::string path(path_);
		path += "/";
		path += this->name_;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << iTotalValue;
		oa << icompressed_vector;
		outfile.flush();
		outfile.close();

		return true;
	}

	template<class T>
	bool DECompressedColumn<T>::load(const std::string& path_){
		this->clearContent();

		std::string path(path_);
		path += "/";
		path += this->name_;
		
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> TotalValue;
		ia >> compressed_vector;
		infile.close();

		return true;
	}

	template<>
	bool DECompressedColumn<float>::load(const std::string& path_){
		this->clearContent();

		std::string path(path_);
		path += "/";
		path += this->name_;
		
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> iTotalValue;
		ia >> icompressed_vector;
		infile.close();

		return true;
	}

	template<class T>
	T& DECompressedColumn<T>::operator[](const int id)
	{
		Return_Variable=0;


		for(int i=0;i<=id;i++)
		{
			Return_Variable+=compressed_vector[i];
		}



		return Return_Variable;
	}

	template<>
	float& DECompressedColumn<float>::operator[](const int id)
	{
		int value=0;

		for(int i=0;i<=id;i++)
		{
			value+=icompressed_vector[i];
		}

		Return_Variable=reinterpret_cast<float*>(&value)[0];

		return Return_Variable;
	}


	template<>
	std::string& DECompressedColumn<std::string>::operator[](const int id)
	{
		Return_Variable="";


		for(int i=0;i<=id;i++)
		{
			Return_Variable=stringadd(Return_Variable,compressed_vector[i]);
		}

		return Return_Variable;
	}

	template<class T>
	unsigned int DECompressedColumn<T>::getSizeinBytes() const throw(){
		return sizeof(T) * compressed_vector.size();
	}

	template<>
	unsigned int DECompressedColumn<float>::getSizeinBytes() const throw(){
		return sizeof(int) * icompressed_vector.size();
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB
