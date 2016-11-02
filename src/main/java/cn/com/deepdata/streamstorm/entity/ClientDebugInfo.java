package cn.com.deepdata.streamstorm.entity;

import java.util.ArrayList;

public class ClientDebugInfo {
	private ArrayList<ClientInfoItem> brand;
	private ArrayList<ClientInfoItem> brand_un;
	private ArrayList<ClientInfoItem> products;
	private ArrayList<ClientInfoItem> products_un;
	private ArrayList<ClientInfoItem> people;
	private ArrayList<ClientInfoItem> people_un;
	private ArrayList<ClientInfoItem> other;
	private ArrayList<ClientInfoItem> other_un;

	public ClientDebugInfo() {
		brand = new ArrayList<>();
		brand_un = new ArrayList<>();
		products = new ArrayList<>();
		products_un = new ArrayList<>();
		people = new ArrayList<>();
		people_un = new ArrayList<>();
		other = new ArrayList<>();
		other_un = new ArrayList<>();
	}

	public ArrayList<ClientInfoItem> getBrand() {
		return brand;
	}

	public void setBrand(ClientInfoItem brand) {
		this.brand.add(brand);
	}

	public ArrayList<ClientInfoItem> getBrand_un() {
		return brand_un;
	}

	public void setBrand_un(ClientInfoItem brand_un) {
		this.brand_un.add(brand_un);
	}

	public ArrayList<ClientInfoItem> getProducts() {
		return products;
	}

	public void setProducts(ClientInfoItem products) {
		this.products.add(products);
	}

	public ArrayList<ClientInfoItem> getProducts_un() {
		return products_un;
	}

	public void setProducts_un(ClientInfoItem products_un) {
		this.products_un.add(products_un);
	}

	public ArrayList<ClientInfoItem> getPeople() {
		return people;
	}

	public void setPeople(ClientInfoItem people) {
		this.people.add(people);
	}

	public ArrayList<ClientInfoItem> getPeople_un() {
		return people_un;
	}

	public void setPeople_un(ClientInfoItem people_un) {
		this.people_un.add(people_un);
	}

	public ArrayList<ClientInfoItem> getOther() {
		return other;
	}

	public void setOther(ClientInfoItem other) {
		this.other.add(other);
	}

	public ArrayList<ClientInfoItem> getOther_un() {
		return other_un;
	}

	public void setOther_un(ClientInfoItem other_un) {
		this.other_un.add(other_un);
	}
}
