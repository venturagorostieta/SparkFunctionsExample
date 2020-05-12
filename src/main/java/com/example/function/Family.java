package com.example.function;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Family implements Serializable {
	
    private int familyId;
    private List<String> membersList;
    public Family() {
    }
    public Family(int familyId, List<String> membersList) {
        this.familyId = familyId;
        this.membersList = membersList;
    }
    public int getFamilyId() {
        return familyId;
    }
    public void setFamilyId(int familyId) {
        this.familyId = familyId;
    }
    public List<String> getMembersList() {
        return membersList;
    }
    public void setMembersList(List<String> membersList) {
        this.membersList = membersList;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Family)) return false;
        Family that = (Family) o;
        return familyId == that.familyId;
    }
    @Override
    public int hashCode() {
        return Objects.hash(familyId);
    }
    @Override
    public String toString() {
        return membersList.toString().replace("[","").replace("]","");
    }

}
