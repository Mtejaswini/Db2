
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
DECompressedColumn<T>::DECompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type),compressed_vector(),TotalValue(0),Return_Variable(0){

}

template<class T>
DECompressedColumn<T>::~DECompressedColumn(){

}

template<class T>
bool DECompressedColumn<T>::insert(const boost::any& new_Value)
{

	T value = boost::any_cast<T>(new_Value);
	this->insert(value);

	return true;

}

template<class T>
bool DECompressedColumn<T>::insert(const T& value)
{

	T insertvalue;
	if(compressed_vector.size()>0)
	{
		//std::cout<<"else Time "<<value<<std::endl;
		insertvalue=value-TotalValue;
		TotalValue+=insertvalue;

	}
	else
	{
		//std::cout<<"First Time "<<value<<std::endl;
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
	//Code for first item add
	T insertvalue;
	insertvalue=start;
	TotalValue=insertvalue;
	if (!this->insert(*insertvalue))
	{
		return false;
	}
	InputIterator startnew=++start;

	//Code for second item onwards
	for ( ; startnew != end; ++startnew)
	{
		insertvalue=startnew-TotalValue;
		TotalValue+=insertvalue;

		if (!this->insert(*insertvalue))
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
	for(TID i=0;i<index;i++)
	{
		value+=compressed_vector[i];
	}

	return value;
}

template<class T>
void DECompressedColumn<T>::print() const throw()
{
	std::cout<<"The Delta encoded data is:"<<std::endl;
	T Total=0;
	for(int i=0; i<compressed_vector.size();i++)
	{
		Total+=compressed_vector[i];
		std::cout<<Total<<std::endl;
	}

}

template<class T>
size_t DECompressedColumn<T>::size() const throw()
{

	return compressed_vector.size();
}

template<class T>
const ColumnPtr DECompressedColumn<T>::copy() const{

	return ColumnPtr(new DECompressedColumn<T>((*this)));
}

template<class T>
bool DECompressedColumn<T>::update(TID id , const boost::any& updatesvalue )
{
	T value = boost::any_cast<T>(updatesvalue);
	if (id >= this->size()) return false;
	if(id==this->size())
	{
		compressed_vector[id]=TotalValue-value;
		TotalValue=compressed_vector[id];
		return false;
	}
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

}



template<class T>
bool DECompressedColumn<T>::update(PositionListPtr , const boost::any& ){
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


template<class T>
bool DECompressedColumn<T>::remove(PositionListPtr){
	return false;
}

template<class T>
bool DECompressedColumn<T>::clearContent(){
	compressed_vector.clear();

	return true;
}
template<class T>
bool DECompressedColumn<T>::store(const std::string& path)
{
	std::ofstream contigFileStream;
	std::string CsvFilePath =path;
	CsvFilePath.append("/DeltaColumn.csv");
	contigFileStream.open(CsvFilePath.c_str());
	contigFileStream << "C_Name" << ","<< std::endl;

	T Total=0;
	for(int i=0; i<compressed_vector.size();i++)
	{
		Total+=compressed_vector[i];
		contigFileStream << Total<<std::endl;

	}


	return true;
}
template<class T>
bool DECompressedColumn<T>::load(const std::string& path){
	compressed_vector.clear();
	TotalValue=0;
	std::string s;
	std::string full_path=path;
	full_path.append("/DeltaColumn.csv");
	std::ifstream f(full_path.c_str());


	std::ifstream  data(full_path.c_str());

	    std::string line;
	    while(std::getline(data,line))
	    {
	        std::stringstream  lineStream(line);
	        T        cell;
	        while(data>>cell)
	        {
	        		this->insert(cell);

	        }
	    }

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

template<class T>
unsigned int DECompressedColumn<T>::getSizeinBytes() const throw(){
	return sizeof(T) * compressed_vector.size();
}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB
