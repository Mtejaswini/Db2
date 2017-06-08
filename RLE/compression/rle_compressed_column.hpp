
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
class RleCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	RleCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~RleCompressedColumn();

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
	std::vector<T> rle_vector;
	std::vector<int> rle_count;
};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	RleCompressedColumn<T>::RleCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), rle_vector(), rle_count(){
		
	}

	template<class T>
	RleCompressedColumn<T>::~RleCompressedColumn(){

	}

	template<class T>
	bool RleCompressedColumn<T>::insert(const boost::any& new_value){
	
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);			 
			 return this->insert(value);
		}
		
		return false;
	}

	template<class T>
	bool RleCompressedColumn<T>::insert(const T& new_value){			
		if(!rle_vector.empty() && (rle_vector[rle_vector.size() - 1] == new_value))
		{
			
			rle_count[rle_count.size() - 1]++;
		}
		else		
		{
			rle_count.push_back(1);
			rle_vector.push_back(new_value);
		}
	
		return true;
	}

	template <typename T> 
	template <typename InputIterator>
	bool RleCompressedColumn<T>::insert(InputIterator first, InputIterator last){
		
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
	const boost::any RleCompressedColumn<T>::get(TID tid){
		if(tid<rle_vector.size())
		{
 			return boost::any(rle_vector[tid * 2]);
		}
		else{
			std::cout << "Fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid  << std::endl;
		}
		return boost::any();
	}

	template<class T>
	void RleCompressedColumn<T>::print() const throw(){
		
		std::cout << "| " << this->name_ << " |" << std::endl;
		std::cout << "________________________" << std::endl;
		int size = 0;		
		for(unsigned i=0;i<rle_count.size();i++){

			
				size += rle_count[i];
				
				
			std::cout << (size-1) << "| " << rle_vector[(i)] << " |" << std::endl;
		}
	}
	template<class T>
	size_t RleCompressedColumn<T>::size() const throw(){

		int size = 0;
		for(unsigned i = 0; i < rle_count.size(); i++)		{
			size += rle_count[i];
		}		

		return size;
	}
	template<class T>
	const ColumnPtr RleCompressedColumn<T>::copy() const{

		return ColumnPtr(new RleCompressedColumn<T>(*this));
	}

	template<class T>
	bool RleCompressedColumn<T>::update(TID tid, const boost::any& new_value){
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			int size = rle_vector.size();
			vector<T> rle_vector_ = rle_vector;
			vector<int> rle_count_ = rle_count;
			clearContent();
			int i = 0;
			T pre_value;
			unsigned counter = 0;
			for(i = 0; i < size; i ++)
			{
				
				for(int j = 0; j < rle_count_[i]; j++)
				{
					
					
					if(counter != tid)
					pre_value = boost::any_cast<T>(rle_vector_[(i)]);
					else
						pre_value = value;
						
					this->insert(pre_value);
					counter++;
				}
				
			}
			
			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		
		return false;
	}

	template<class T>
	bool RleCompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){
	
		if(!tids)
			return false;
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			T value = boost::any_cast<T>(new_value);
			int size = rle_vector.size();
			vector<T> rle_vector_ = rle_vector;
			vector<int> rle_count_ = rle_count;
			clearContent();
			int i = 0;
			T pre_value;
			unsigned counter = 0;

			for(unsigned int id=0;id<tids->size();id++){
				TID tid=(*tids)[id];

				for(i = 0; i < size; i ++)			{
				
					for(int j = 0; j < rle_count_[i]; j++)
					{					
						if(counter != tid)
						{
							pre_value = boost::any_cast<T>(rle_vector_[(i)]);
							this->insert(pre_value);
						}
						else {
							this->insert(value);
						}
						counter++;
					}
				
				}
			}

			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		
		return false;		
	}
	
	template<class T>
	bool RleCompressedColumn<T>::remove(TID tid){
			int size = rle_vector.size();
			vector<T> rle_vector_ = rle_vector;
			vector<int> rle_count_ = rle_count;
			clearContent();
			int i = 0;
			T pre_value;
			unsigned counter = 0;
			for(i = 0; i < size; i ++)			{
				
				for(int j = 0; j < rle_count_[i]; j++)
				{					
					if(counter != tid)
					{
						pre_value = boost::any_cast<T>(rle_vector_[(i)]);
						this->insert(pre_value);
					}
					counter++;
				}
				
			}
		return true;	
	}
	
	template<class T>
	bool RleCompressedColumn<T>::remove(PositionListPtr tids){
	
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
	bool RleCompressedColumn<T>::clearContent(){
	
		rle_vector.clear();
		rle_count.clear();
		
		return true;
	}

	template<class T>
	bool RleCompressedColumn<T>::store(const std::string& path_){
	
		std::string path(path_);
		path += "/";
		path += this->name_;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << rle_vector;

		outfile.flush();
		outfile.close();


		std::string path2(path_);
		path2 += "/count";
		path2 += this->name_;
		std::ofstream outfile2 (path2.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa2(outfile2);

		oa2 << rle_count;

		outfile2.flush();
		outfile2.close();
		
		return true;
	}
	template<class T>
	bool RleCompressedColumn<T>::load(const std::string& path_){
	
		std::string path(path_);
		path += "/";
		path += this->name_;
		
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> rle_vector;
		infile.close();


		std::string path2(path_);
		path2 += "/count";
		path2 += this->name_;
		std::ifstream infile2 (path2.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia2(infile2);
		ia2 >> rle_count;
		infile2.close();


		return true;
	}

	template<class T>
	T& RleCompressedColumn<T>::operator[](const int index){
		int size = 0;
		unsigned i = 0;
		for(i = 0; i < rle_count.size(); i++)		{
			size += rle_count[i];
			if((size - 1) >= index)
				break;
		}
		return rle_vector[(i)];
	}

	template<class T>
	unsigned int RleCompressedColumn<T>::getSizeinBytes() const throw(){
		return rle_vector.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

